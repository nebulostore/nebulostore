package org.nebulostore.async;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Module that tries to download synchro peer set of peer_ from DHT and updates this peer's set in
 * context.
 *
 * @author Piotr Malicki
 *
 */
public class SynchroPeerSetUpdateModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(SynchroPeerSetUpdateModule.class);

  private final MessageVisitor<Void> visitor_ = new SynchroPeerSetUpdateVisitor();
  private final CommAddress peer_;
  private final AsyncMessagesContext context_;

  public SynchroPeerSetUpdateModule(CommAddress peer, AsyncMessagesContext context) {
    peer_ = peer;
    context_ = context;
  }

  protected class SynchroPeerSetUpdateVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      if (context_.isInitialized()) {
        networkQueue_.add(new GetDHTMessage(jobId_, peer_.toKeyDHT()));
      } else {
        logger_.warn("Asynchronous messages context is not initialized. Ending the module.");
        endJobModule();
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(peer_.toKeyDHT())) {
        if (message.getValue().getValue() instanceof InstanceMetadata) {
          InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
          context_.updateSynchroGroup(peer_, metadata.getSynchroGroup());
        } else {
          logger_.warn("Received ValueDHTMessage with a wrong key.");
        }
      } else {
        logger_.warn("Wrong type of value in ValueDHTMessage.");
      }
      endJobModule();
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.warn("Could not download InstanceMetadata for address " + peer_);
      context_.removeSynchroGroup(peer_);
      endJobModule();
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
