package org.nebulostore.async;

import java.util.Set;

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
 * Module that tries to download synchro peer set of peer_ from DHT and returns it (or null
 * in case of error).
 *
 * @author Piotr Malicki
 *
 */
public class SynchroPeerSetReturningJobModule extends ReturningJobModule<Set<CommAddress>> {

  private static Logger logger_ = Logger.getLogger(SynchroPeerSetReturningJobModule.class);

  private final MessageVisitor<Void> visitor_ = new SynchroPeerSetReturnVisitor();
  private final CommAddress peer_;

  public SynchroPeerSetReturningJobModule(CommAddress peer) {
    peer_ = peer;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected class SynchroPeerSetReturnVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      networkQueue_.add(new GetDHTMessage(jobId_, peer_.toKeyDHT()));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(peer_.toKeyDHT())) {
        if (message.getValue().getValue() instanceof InstanceMetadata) {
          InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
          endWithSuccess(metadata.getSynchroGroup());
        } else {
          logger_.warn("Received ValueDHTMessage with a wrong key.");
        }
      } else {
        logger_.warn("Wrong type of value in ValueDHTMessage.");
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.warn("Could not download InstanceMetadata for address " + peer_);
      endWithSuccess(null);
      return null;
    }
  }

}
