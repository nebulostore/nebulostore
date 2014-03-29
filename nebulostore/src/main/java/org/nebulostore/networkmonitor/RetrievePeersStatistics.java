package org.nebulostore.networkmonitor;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

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
    ReturningJobModule<ConcurrentLinkedQueue<PeerConnectionSurvey>> {
  private static Logger logger_ = Logger.getLogger(RetrievePeersStatistics.class);
  private final CommAddress peer_;

  public RetrievePeersStatistics(CommAddress peer, BlockingQueue<Message> dispatcherQueue) {
    peer_ = peer;
    outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  private final RPSVisitor visitor_ = new RPSVisitor();

  /**
   * Visitor.
   */
  public class RPSVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      networkQueue_.add(new GetDHTMessage(message.getId(), peer_.toKeyDHT()));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
      logger_.debug("Retrived peers " + peer_ + " statistics");
      for (PeerConnectionSurvey pcs : metadata.getStatistics()) {
        logger_.debug(pcs.toString());
      }
      endWithSuccess(metadata.getStatistics());
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      endWithError(new NebuloException("DHT Error"));
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
