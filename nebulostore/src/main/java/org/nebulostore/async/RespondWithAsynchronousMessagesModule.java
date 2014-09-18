package org.nebulostore.async;

import java.util.List;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.messages.GotAsynchronousMessagesMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;
import org.nebulostore.utils.Pair;

/**
 * Response for GetAsynchronousDataMessage.
 *
 * @author Piotr Malicki
 */
public class RespondWithAsynchronousMessagesModule extends JobModule {

  private static final int ACK_TIMEOUT = 10000;
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
   * Visitor.
   *
   * @author szymonmatejczyk
   * @author Piotr Malicki
   */
  protected class RespondWithAsyncVisitor extends MessageVisitor<Void> {
    private CommAddress askingPeer_;
    private CommAddress recipient_;

    public Void visit(GetAsynchronousMessagesMessage message) {
      if (context_.isInitialized()) {
        askingPeer_ = message.getSourceAddress();
        recipient_ = message.getRecipient();

        if (context_.containsRecipient(message.getRecipient())) {
          Pair<CommAddress, CommAddress> waitingForAckEntry = new Pair<>(askingPeer_, recipient_);
          if (context_.testAndAddWaitingForAck(waitingForAckEntry)) {
            logger_.info("Ignoring request for asynchronous messages of peer " +
                message.getRecipient() + " from peer " + message.getSourceAddress() +
                ". We are already waiting for ack message.");
          } else {
            jobId_ = message.getId();
            // TODO(szm): prevent message flooding
            List<AsynchronousMessage> messagesForPeer =
                context_.getMessagesForPeerListCopy(recipient_);
            AsynchronousMessagesMessage reply =
                new AsynchronousMessagesMessage(message.getId(), message.getDestinationAddress(),
                    askingPeer_, messagesForPeer, recipient_);
            networkQueue_.add(reply);
            timer_.schedule(jobId_, ACK_TIMEOUT);
          }
        } else {
          logger_
              .warn("GetAsynchronousMessagesMessage received from peer not in any of our groups");
        }
      } else {
        logger_.warn("Async messages context has not yet been initialized, ending the module");
        endJobModule();
      }
      return null;
    }

    public Void visit(GotAsynchronousMessagesMessage message) {
      // We assume that if Peer asks for AM to him, there won't be new messages
      // for him.
      if (context_.removeAckWaitingEntry(new Pair<>(message.getSourceAddress(), message
          .getRecipient()))) {
        /*
         * TODO (pm) In the next version of module messages won't be removed in that manner.
         */
        context_.removeWaitingMessagesForPeer(message.getRecipient());
        logger_.debug(message.getRecipient().toString() +
            " successfully downloaded asynchronous messages.");
      } else {
        logger_.warn("Got ACK, that shouldn't be sent.");
      }
      endJobModule();
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout in " + getClass());
      if (askingPeer_ == null || recipient_ == null) {
        logger_.warn("Received TimeoutMessage which was not expected");
      } else {
        context_.removeAckWaitingEntry(new Pair<>(askingPeer_, recipient_));
        endJobModule();
      }
      return null;
    }
  }
}
