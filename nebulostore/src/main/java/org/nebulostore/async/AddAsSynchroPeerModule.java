package org.nebulostore.async;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AddAsSynchroPeerMessage;
import org.nebulostore.async.messages.AddedAsSynchroPeerMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;

public class AddAsSynchroPeerModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(AddAsSynchroPeerModule.class);

  private final MessageVisitor<Void> visitor_ = new AddAsSynchroPeerVisitor();
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

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected class AddAsSynchroPeerVisitor extends MessageVisitor<Void> {

    public Void visit(AddAsSynchroPeerMessage message) {
      messageJobId_ = message.getId();
      recipient_ = message.getSourceAddress();
      InstanceMetadata metadata = new InstanceMetadata(appKey_);
      metadata.getRecipients().add(message.getSourceAddress());
      networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
      return null;
    }

    public Void visit(OkDHTMessage message) {
      networkQueue_.add(new AddedAsSynchroPeerMessage(messageJobId_, myAddress_, recipient_));
      context_.acquireInboxHoldersWriteRights();
      context_.getRecipients().add(recipient_);
      context_.freeInboxHoldersWriteRights();
      endJobModule();
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_
          .warn("Adding current instance as a synchro-peer of peer " + recipient_ + " failed.");
      return null;
    }
  }
}
