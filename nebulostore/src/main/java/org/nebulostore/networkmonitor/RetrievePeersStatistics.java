package org.nebulostore.networkmonitor;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Module that queries DHT for peer's statistics.
 *
 * @author szymonmatejczyk
 */
public class RetrievePeersStatistics extends
    ReturningJobModule<StatisticsList> {
  private static Logger logger_ = Logger.getLogger(RetrievePeersStatistics.class);
  private final CommAddress peer_;

  public RetrievePeersStatistics(CommAddress peer, BlockingQueue<Message> dispatcherQueue) {
    peer_ = peer;
    outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  private final RPSVisitor visitor_ = new RPSVisitor();

  public class RPSVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      jobId_ = message.getId();
      networkQueue_.add(new GetDHTMessage(message.getId(), peer_.toKeyDHT()));
    }

    public void visit(ValueDHTMessage message) {
      InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
      logger_.debug("Retrived peers " + peer_ + " statistics");
      for (PeerConnectionSurvey pcs : metadata.getStatistics().getAllStatisticsView()) {
        logger_.debug(pcs.toString());
      }
      endWithSuccess(metadata.getStatistics());
    }

    public void visit(ErrorDHTMessage message) {
      endWithError(new NebuloException("DHT Error"));
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
