package org.nebulostore.networkmonitor;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestResponseMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Tests peers connection data(ex. availability, bandwidth, ...). Appends result to DHT.
 *
 * @author szymonmatejczyk
 */
public class TestPeersConnectionModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(TestPeersConnectionModule.class);
  private static final long TIMEOUT_MILLIS = 3000L;

  private final CommAddress peerAddress_;
  private TPCVisitor visitor_ = new TPCVisitor();
  private Timer timer_;
  private CommAddress myAddress_;

  public TestPeersConnectionModule(CommAddress peer, BlockingQueue<Message> dispatcherQueue) {
    peerAddress_ = peer;
    outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  @Inject
  public void setDependencies(Timer timer, CommAddress commAddress) {
    timer_ = timer;
    myAddress_ = commAddress;
  }

  long sendTime_;
  private ValueDHTMessage valueDHTMessage_;
  private List<PeerConnectionSurvey> stats_ = new LinkedList<PeerConnectionSurvey>();

  /**
   * Visitor.
   */
  public class TPCVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) {
      logger_.debug("Testing connection to: " + peerAddress_.toString());
      networkQueue_.add(new GetDHTMessage(message.getId(), peerAddress_.toKeyDHT()));
      sendTime_ = System.currentTimeMillis();
      networkQueue_.add(new ConnectionTestMessage(jobId_, peerAddress_));
      timer_.schedule(jobId_, TIMEOUT_MILLIS);
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.warn("Unable to retrive statistics from DHT: " + message.getException());
      timer_.cancelTimer();
      endJobModule();
      return null;
    }

    public Void visit(ConnectionTestResponseMessage message) {
      logger_.debug("Succesfully tested connection to: " + peerAddress_.toString());
      // TODO(szm): other statistics
      // TODO(szm): bandwidth??
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.AVAILABILITY, 1.0));
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.LATENCY, (System.currentTimeMillis() - sendTime_) / 2.0));

      if (valueDHTMessage_ != null) {
        appendStatisticsAndFinish(stats_, valueDHTMessage_);
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (!stats_.isEmpty()) {
        appendStatisticsAndFinish(stats_, message);
      } else {
        valueDHTMessage_ = message;
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      if (valueDHTMessage_ != null) {
        logger_.debug("Timeout in ping.");
        stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
            ConnectionAttribute.AVAILABILITY, 0.0));
        appendStatisticsAndFinish(stats_, valueDHTMessage_);
      } else {
        logger_.warn("Timeout in DHT retrival.");
        endJobModule();
      }
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      logger_.warn("Got ErrorCommMessage: " + message.getNetworkException());
      if (valueDHTMessage_ != null) {
        stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
            ConnectionAttribute.AVAILABILITY, 0.0));
        appendStatisticsAndFinish(stats_, valueDHTMessage_);
      } else {
        endJobModule();
      }
      return null;
    }
  }

  private void appendStatisticsAndFinish(List<PeerConnectionSurvey> stats, ValueDHTMessage message)
  {
    timer_.cancelTimer();
    InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
    for (PeerConnectionSurvey pcs : stats) {
      logger_.debug("Adding to DHT: " + pcs.toString());
      metadata.getStatistics().add(pcs);
    }
    // DHT synchronization is ensured by merge operation in InstanceMetadata
    networkQueue_
        .add(new PutDHTMessage(getJobId(), peerAddress_.toKeyDHT(), new ValueDHT(metadata)));
    endJobModule();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
