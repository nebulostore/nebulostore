package org.nebulostore.async.synchrogroup;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.messages.AsyncModuleErrorMessage;
import org.nebulostore.async.synchrogroup.messages.RemoveFromSynchroPeerSetMessage;
import org.nebulostore.async.util.RecipientsData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;

/**
 * Module responsible for removing current instance from the synchro-peer set of recipient_.
 *
 * We ensure that real recipients set is always a subset of current instance's
 * recipients set stored in both DHT and memory.
 *
 * @author Piotr Malicki
 *
 */
public class RemoveFromSynchroPeerSetModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(RemoveFromSynchroPeerSetModule.class);

  private final MessageVisitor visitor_ = new RemoveFromSetVisitor();
  private CommAddress recipient_;

  private AsyncMessagesContext context_;
  private CommAddress myAddress_;

  @Inject
  public void setDependencies(AsyncMessagesContext context, CommAddress myAddress) {
    context_ = context;
    myAddress_ = myAddress;
  }

  protected class RemoveFromSetVisitor extends MessageVisitor {

    public void visit(RemoveFromSynchroPeerSetMessage message) {
      if (context_.isInitialized()) {
        if (context_.lockRecipient(message.getSourceAddress())) {
          if (context_.containsRecipient(message.getSourceAddress())) {
            recipient_ = message.getSourceAddress();
            context_.removeRecipient(recipient_);

            RecipientsData recipientsData = context_.getRecipientsData();
            InstanceMetadata metadata = new InstanceMetadata();
            metadata.setRecipients(recipientsData.getRecipients());
            metadata.setRecipientsSetVersion(recipientsData.getRecipientsSetVersion());
            networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(),
                new ValueDHT(metadata)));
          } else {
            logger_.warn("Received " + message.getClass() + " from peer " +
                recipient_ + " which is not present in recipients set");
            networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, recipient_));
            context_.freeRecipient(message.getSourceAddress());
            endJobModule();
          }
        } else {
          logger_.warn("Peer " + recipient_ + " is already locked, cancelling the" +
              "request");
          networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, recipient_));
          endJobModule();
        }
      } else {
        logger_
            .warn("Asynchronous messages context has not yet been initialized. Ending the module.");
        networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, recipient_));
        endJobModule();
      }
    }

    public void visit(OkDHTMessage message) {
      logger_.info("Successfully removed peer " + recipient_ + " from recipients set in DHT");
      context_.freeRecipient(recipient_);
      endJobModule();
    }

    public void visit(ErrorDHTMessage message) {
      logger_.warn("Removing peer " + recipient_ + " from recipients set in DHT failed.");
      context_.freeRecipient(recipient_);
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
