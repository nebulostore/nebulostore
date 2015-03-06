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
import org.nebulostore.dht.messages.PutDHTMessage;
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
  private final TPCVisitor visitor_ = new TPCVisitor();
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
  private final List<PeerConnectionSurvey> stats_ = new LinkedList<PeerConnectionSurvey>();

  public class TPCVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Testing connection to: " + peerAddress_.toString());
      sendTime_ = System.currentTimeMillis();
      networkQueue_.add(new ConnectionTestMessage(jobId_, peerAddress_));
      timer_.schedule(jobId_, TIMEOUT_MILLIS);
    }

    public void visit(ConnectionTestResponseMessage message) {
      logger_.debug("Succesfully tested connection to: " + peerAddress_.toString());
      // TODO(szm): other statistics
      // TODO(szm): bandwidth??
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.AVAILABILITY, 1.0));
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.LATENCY, (System.currentTimeMillis() - sendTime_) / 2.0));

      appendStatisticsAndFinish(stats_);
    }

    public void visit(TimeoutMessage message) {
      logger_.debug("Timeout in ping.");
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.AVAILABILITY, 0.0));
      appendStatisticsAndFinish(stats_);
    }

    public void visit(ErrorCommMessage message) {
      logger_.warn("Got ErrorCommMessage: " + message.getNetworkException());
      stats_.add(new PeerConnectionSurvey(myAddress_, System.currentTimeMillis(),
          ConnectionAttribute.AVAILABILITY, 0.0));
      appendStatisticsAndFinish(stats_);
    }
  }

  private void appendStatisticsAndFinish(List<PeerConnectionSurvey> stats) {
    timer_.cancelTimer();
    InstanceMetadata metadata = new InstanceMetadata();
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
