package org.nebulostore.async;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.StoreAsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Sends asynchronous message to all peers' synchro-peers.
 *
 * We give no guarantee on asynchronous messages.
 *
 * @author szymonmatejczyk
 *
 */
public class SendAsynchronousMessagesForPeerModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(SendAsynchronousMessagesForPeerModule.class);

  private final CommAddress recipient_;
  private final AsynchronousMessage message_;
  private CommAddress myAddress_;
  private AsyncMessagesContext context_;

  public SendAsynchronousMessagesForPeerModule(CommAddress recipient, AsynchronousMessage message,
      BlockingQueue<Message> dispatcherQueue) {
    recipient_ = recipient;
    message_ = message;
    outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  @Inject
  public void setDependencies(CommAddress myAddress, AsyncMessagesContext context) {
    myAddress_ = myAddress;
    context_ = context;
  }

  private final MessageVisitor<Void> visitor_ = new SendAsynchronousMessagesForPeerModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Visitor.
   */
  public class SendAsynchronousMessagesForPeerModuleVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      if (context_.isInitialized()) {
        jobId_ = message.getId();
        networkQueue_.add(new GetDHTMessage(jobId_, recipient_.toKeyDHT()));
      } else {
        logger_.warn("Async messages context has not yet been initialized, ending the module");
        endJobModule();
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(recipient_.toKeyDHT()) &&
          (message.getValue().getValue() instanceof InstanceMetadata)) {
        InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
        for (CommAddress inboxHolder : metadata.getSynchroGroup()) {
          if (inboxHolder.equals(myAddress_)) {
            context_.addWaitingAsyncMessage(recipient_, message_);
          } else if (!inboxHolder.equals(recipient_)) {
            networkQueue_.add(new StoreAsynchronousMessage(jobId_, null, inboxHolder, recipient_,
                message_));
          }
        }
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.error("Sending asynchronous messages for " + recipient_ + " failed...");
      return null;
    }
  }

}
