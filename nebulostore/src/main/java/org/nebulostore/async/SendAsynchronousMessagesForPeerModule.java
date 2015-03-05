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
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * The module responsible for sending an asynchronous message to all synchro-peers of the
 * destination peer.
 *
 * @author szymonmatejczyk
 * @author Piotr Malicki
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

  private final MessageVisitor visitor_ = new SendAsynchronousMessagesForPeerModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  public class SendAsynchronousMessagesForPeerModuleVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      logger_.debug("Starting " + getClass().getName() + " with message: " + message_ + "for: " +
          recipient_);
      networkQueue_.add(new GetDHTMessage(jobId_, recipient_.toKeyDHT()));
    }

    public void visit(ValueDHTMessage message) {
      logger_.debug("Received " + message + " in " + getClass().getName());
      if (message.getKey().equals(recipient_.toKeyDHT()) &&
          (message.getValue().getValue() instanceof InstanceMetadata)) {
        InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
        if (metadata == null || metadata.getSynchroGroup() == null ||
            metadata.getSynchroGroup().size() == 0) {
          logger_.warn("Could not send message asynchronously, because recipient's synchro peer " +
              " set is empty");
        } else {
          for (CommAddress inboxHolder : metadata.getSynchroGroup()) {
            if (context_.isInitialized() && inboxHolder.equals(myAddress_)) {
              context_.storeAsynchronousMessage(recipient_, message_);
            } else if (!inboxHolder.equals(recipient_)) {
              logger_.debug("Sending asynchronous message: " + message_ + " to peer: " +
                  inboxHolder);
              CommMessage msg =
                  new StoreAsynchronousMessage(jobId_, null, inboxHolder, recipient_, message_);
              networkQueue_.add(msg);
            }
          }
        }
        endJobModule();
      } else {
        logger_.warn("Received " + message.getClass().getName() + " that was not expected");
      }
      endJobModule();
    }

    public void visit(ErrorDHTMessage message) {
      logger_.error("Sending asynchronous messages for " + recipient_ + " failed...");
      endJobModule();
    }
  }

}
