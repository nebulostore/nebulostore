package org.nebulostore.async.synchronization;

import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.messages.AsyncModuleErrorMessage;
import org.nebulostore.async.synchronization.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.synchronization.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.util.RecipientPeerData;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Response for GetAsynchronousMessagesMessage.
 *
 * @author Piotr Malicki
 */
public class RespondWithAsynchronousMessagesModule extends JobModule {

  private static final int MESSAGES_TIMEOUT = 10000;
  private static Logger logger_ = Logger.getLogger(RespondWithAsynchronousMessagesModule.class);

  private AsyncMessagesContext context_;
  private Timer timer_;

  @Inject
  public void setDependencies(AsyncMessagesContext context, Timer timer) {
    context_ = context;
    timer_ = timer;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private final RespondWithAsyncVisitor visitor_ = new RespondWithAsyncVisitor();

  /**
   * @author szymonmatejczyk
   * @author Piotr Malicki
   */
  protected class RespondWithAsyncVisitor extends MessageVisitor {
    private CommAddress askingPeer_;
    private CommAddress recipient_;

    public void visit(GetAsynchronousMessagesMessage message) {
      logger_.info("Received request for async messages from " + message.getSourceAddress() +
          " for " + message.getRecipient());
      if (context_.isInitialized()) {
        askingPeer_ = message.getSourceAddress();
        recipient_ = message.getRecipient();

        RecipientPeerData peerData = context_.getRecipientPeerDataForPeer(recipient_);
        Set<CommAddress> synchroGroup = context_.getSynchroGroupForPeerCopy(recipient_);
        if (peerData.isRecipient()) {
          if ((synchroGroup != null && synchroGroup.contains(message.getSourceAddress())) ||
              message.getSourceAddress().equals(recipient_)) {

            // TODO(szm): prevent message flooding
            peerData.removeMessagesFromBeforeTimestamp(message.getTimestamp());

            AsynchronousMessagesMessage reply =
                new AsynchronousMessagesMessage(message.getId(), message.getDestinationAddress(),
                    askingPeer_, peerData.getMessages(), recipient_, peerData.getClockValue(),
                    peerData.getLastClearTimestamp(), peerData.getMessagesTimestamps());
            logger_.info("Sending messages for peer " + recipient_ + "requested by peer " +
                askingPeer_ + ", messages: " + reply.getMessages());
            networkQueue_.add(reply);
            timer_.schedule(jobId_, MESSAGES_TIMEOUT);
          } else {
            logger_.warn("Received GetAsynchronousMessages request from peer which is not a " +
                "synchro-peer of given recipient. Ending the module.");
            finishModule();
          }
        } else {
          logger_.warn("Received GetAsynchronousMessages request for peer that is not a " +
              "recipient of current instance.");
          networkQueue_.add(new AsyncModuleErrorMessage(message.getDestinationAddress(), message
              .getSourceAddress()));
          finishModule();
        }
      } else {
        logger_.warn("Asynchronous messages context has not yet been initialized.");
        networkQueue_.add(new AsyncModuleErrorMessage(message.getDestinationAddress(), message
            .getSourceAddress()));
        finishModule();
      }
    }

    public void visit(AsynchronousMessagesMessage message) {
      logger_.debug("Received asynchronous messages from " + message.getSourceAddress() + " for " +
          message.getRecipient());
      if (message.getSourceAddress().equals(askingPeer_) &&
          message.getRecipient().equals(recipient_)) {
        context_.updateRecipientDataForPeer(recipient_, message.getMessages(),
            message.getMessagesTimestamps(), message.getLastClearTimestamp(),
            message.getTimestamp());
        endJobModule();
      } else {
        logger_.warn("Got " + message.getClass().getName() + " that shouldn't be sent.");
      }
      finishModule();
    }

    public void visit(TimeoutMessage message) {
      logger_.warn("Timeout in " + getClass());
      if (askingPeer_ == null || recipient_ == null) {
        logger_.warn("Received TimeoutMessage which was not expected");
      } else {
        endJobModule();
      }
    }

    public void visit(ErrorCommMessage message) {
      logger_.warn("Message " + message.getMessage() + " has not been sent.");
      endJobModule();
    }

    public void finishModule() {
      timer_.cancelTimer();
      endJobModule();
    }
  }
}
