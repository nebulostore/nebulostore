package org.nebulostore.async;

import com.google.inject.Inject;

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
 * This module tries to load asynchronous messages context from DHT by downloading
 * InstanceMetadata. If it's not present there, creates new empty context.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncMessagesContextInitializationModule extends JobModule {

  private static final Logger LOGGER =
      Logger.getLogger(AsyncMessagesContextInitializationModule.class);

  private final MessageVisitor<Void> visitor_ = new ContextInitVisitor();
  private CommAddress myAddress_;
  private AsyncMessagesContext context_;

  @Inject
  public void setDependencies(CommAddress myAddress, AsyncMessagesContext context) {
    myAddress_ = myAddress;
    context_ = context;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected class ContextInitVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) {
      /*
       * TODO (pm) Make sure that this module will be first to acquire
       * write rights for inbox holders map
       */
      context_.acquireInboxHoldersWriteRights();
      networkQueue_.add(new GetDHTMessage(jobId_, myAddress_.toKeyDHT()));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(myAddress_.toKeyDHT())) {
        if (message.getValue().getValue() instanceof InstanceMetadata) {
            InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
            context_.getSynchroGroupForPeer(myAddress_).addAll(metadata.getSynchroGroup());
            context_.getRecipients().addAll(metadata.getRecipients());
            context_.freeInboxHoldersWriteRights();
            endJobModule();
        } else {
          LOGGER.warn("Received wrong type of message from DHT");
        }
      } else {
        LOGGER.warn("Received " + message.getClass() + " that was not expected");
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      //no instance metadata in DHT, create empty context
      context_.initialize();
      context_.freeInboxHoldersWriteRights();
      endJobModule();
      return null;
    }
  }

}
