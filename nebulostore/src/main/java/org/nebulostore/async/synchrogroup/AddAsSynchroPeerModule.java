package org.nebulostore.async.synchrogroup;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.messages.AsyncModuleErrorMessage;
import org.nebulostore.async.synchrogroup.messages.AddAsSynchroPeerMessage;
import org.nebulostore.async.synchrogroup.messages.AddedAsSynchroPeerMessage;
import org.nebulostore.async.util.RecipientsData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;

/**
 * Module responsible for adding current instance as a synchro-peer of recipient_.
 *
 * We ensure that real recipients set is always a subset of current instance's recipients set stored
 * in both DHT and memory.
 *
 * @author Piotr Malicki
 *
 */
public class AddAsSynchroPeerModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(AddAsSynchroPeerModule.class);

  private final MessageVisitor visitor_ = new AddAsSynchroPeerVisitor();
  private String messageJobId_;
  private CommAddress recipient_;

  private AppKey appKey_;
  private CommAddress myAddress_;
  private AsyncMessagesContext context_;

  @Inject
  public void setDependencies(AppKey appKey, CommAddress myAddress, AsyncMessagesContext context) {
    appKey_ = appKey;
    myAddress_ = myAddress;
    context_ = context;
  }

  protected class AddAsSynchroPeerVisitor extends MessageVisitor {

    public void visit(AddAsSynchroPeerMessage message) {
      logger_.debug("Starting " + AddAsSynchroPeerModule.class + " with message: " + message);
      if (context_.isInitialized()) {
        if (context_.lockRecipient(message.getSourceAddress())) {
          messageJobId_ = message.getId();
          recipient_ = message.getSourceAddress();
          if (context_.addRecipient(recipient_, message.getCounterValue())) {
            RecipientsData recipientsData = context_.getRecipientsData();
            InstanceMetadata metadata = new InstanceMetadata(appKey_);
            metadata.setRecipients(recipientsData.getRecipients());
            metadata.setRecipientsSetVersion(recipientsData.getRecipientsSetVersion());
            networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(
                metadata)));
          } else {
            logger_.info("Could not add new recipient: too many recipients");
            networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, message.getSourceAddress()));
            endJobModule();
          }
        } else {
          logger_.warn("Peer " + message.getSourceAddress() + " is already locked, cancelling the" +
              "request");
          networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, message.getSourceAddress()));
          endJobModule();
        }
      } else {
        logger_.warn("Async messages context has not yet been initialized, ending the module");
        networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, message.getSourceAddress()));
        endJobModule();
      }
    }

    public void visit(OkDHTMessage message) {
      logger_.debug("Added new recipients set to DHT");
      networkQueue_.add(new AddedAsSynchroPeerMessage(messageJobId_, myAddress_, recipient_));
      context_.freeRecipient(recipient_);
      endJobModule();
    }

    public void visit(ErrorDHTMessage message) {
      logger_.warn("Adding current instance as a synchro-peer of peer " + recipient_ + " failed.");
      networkQueue_.add(new AsyncModuleErrorMessage(myAddress_, recipient_));
      context_.removeRecipient(recipient_);
      context_.freeRecipient(recipient_);
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
