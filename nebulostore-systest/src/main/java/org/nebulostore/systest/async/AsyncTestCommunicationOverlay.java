package org.nebulostore.systest.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.Module;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.SendResult;
import org.nebulostore.communication.routing.errorresponder.ErrorResponder;
import org.nebulostore.conductor.messages.FinishMessage;
import org.nebulostore.conductor.messages.TicMessage;
import org.nebulostore.conductor.messages.TocMessage;
import org.nebulostore.dht.messages.InDHTMessage;
import org.nebulostore.dht.messages.OutDHTMessage;
import org.nebulostore.systest.async.messages.CheckCommunicationStateMessage;
import org.nebulostore.systest.async.messages.DisableCommunicationMessage;
import org.nebulostore.systest.async.messages.EnableCommunicationMessage;
import org.nebulostore.systest.async.messages.ResponseWithCommunicationStateMessage;

/**
 * Communication overlay for asynchronous messages systest. This module is used to pretend that
 * communication module is disabled. It catches all messages sent between dispatcher and
 * communication modules.<br>
 * <br>
 * When isCommEnabled_ is set to true:<br>
 * - all messages from the communication module are forwarded to the dispatcher<br>
 * - before sending a message to communication module overlay asks destination peer's overlay for
 * its communication state. If receiver's communication is enabled, overlay forwards the message to
 * the communication module. In the other case, it runs error responder generetad by this
 * message.<br>
 * <br>
 * When isCommEnabled_ is set to false:<br>
 * - all messages from the communication module are forwarded to the dispatcher<br>
 * - all messages from the dispatcher are ignored
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestCommunicationOverlay extends Module {

  private static Logger logger_ = Logger.getLogger(AsyncTestCommunicationOverlay.class);

  private final ExecutorService executor_;
  /**
   * List of messages waiting for message indicating receiver's communication state.
   */
  private final ConcurrentMap<CommAddress, List<CommMessage>> waitingMessages_;
  private boolean isCommEnabled_;
  private final BlockingQueue<Message> communicationInQueue_;
  private final AsyncTestCommOverlayVisitor visitor_;
  private final CommAddress myAddress_;

  @Inject
  public AsyncTestCommunicationOverlay(@Named("CommOverlayInQueue") BlockingQueue<Message> inQueue,
      @Named("CommOverlayOutQueue") BlockingQueue<Message> outQueue,
      @Named("CommOverlayNetworkQueue") BlockingQueue<Message> communicationInQueue,
      CommAddress myAddress) {
    super(inQueue, outQueue);
    communicationInQueue_ = communicationInQueue;
    executor_ = Executors.newFixedThreadPool(8);
    waitingMessages_ = new ConcurrentHashMap<>();
    isCommEnabled_ = true;
    visitor_ = new AsyncTestCommOverlayVisitor();
    myAddress_ = myAddress;
  }

  protected final class AsyncTestCommOverlayVisitor extends MessageVisitor<Void> {

    public Void visit(CheckCommunicationStateMessage message) {
      ResponseWithCommunicationStateMessage msg = new ResponseWithCommunicationStateMessage(
          message.getDestinationAddress(), message.getSourceAddress(), isCommEnabled_,
          message.getOriginalMessageId());
      communicationInQueue_.add(msg);
      return null;
    }

    public Void visit(ResponseWithCommunicationStateMessage message) {
      synchronized (waitingMessages_) {
        List<CommMessage> messagesToSend = waitingMessages_.remove(message.getSourceAddress());
        if (messagesToSend != null) {
          if (message.isCommEnabled()) {
            // now we can send the message
            for (final CommMessage msg : messagesToSend) {
              communicationInQueue_.add(msg);
            }
          } else {
            // we have to send an asynchronous message
            for (final CommMessage msg : messagesToSend) {
              final ErrorResponder errorResponder = msg.generateErrorResponder(outQueue_);
              errorResponder.handleError(new SendResult(msg));
            }
          }
        }
      }
      return null;
    }

    public Void visit(DisableCommunicationMessage msg) {
      isCommEnabled_ = false;
      return null;
    }

    public Void visit(EnableCommunicationMessage msg) {
      isCommEnabled_ = true;
      return null;
    }

    public Void visit(TicMessage msg) {
      return visitMessageNoCheck(msg);
    }

    public Void visit(TocMessage msg) {
      return visitMessageNoCheck(msg);
    }

    public Void visit(FinishMessage msg) {
      return visitMessageNoCheck(msg);
    }

    public Void visit(ErrorCommMessage message) {
      logger_.warn("Message " + message.getMessage() + " was not sent because of" + "an error.");
      return null;
    }

    public Void visit(CommMessage msg) {

      if (msg.getDestinationAddress() == null) {
        logger_.warn("Received message with empty destination address. Dropping this message.");
      } else if (msg.getDestinationAddress().equals(myAddress_)) {
        /*
         * Message from another peer. We assume that message sender had checked our communication
         * state before sending the message or this type of message doesn't require checks.
         */
        outQueue_.add(msg);
      } else if (isCommEnabled_) {
        executor_.submit(new AsyncTestCommOverlayCallable(msg));
      }
      return null;
    }

    public Void visit(InDHTMessage msg) {
      communicationInQueue_.add(msg);
      return null;
    }

    public Void visit(OutDHTMessage msg) {
      outQueue_.add(msg);
      return null;
    }

    public Void visit(EndModuleMessage msg) {
      endModule();
      return null;
    }

    private Void visitMessageNoCheck(CommMessage msg) {
      if (msg.getDestinationAddress().equals(myAddress_)) {
        outQueue_.add(msg);
      } else {
        communicationInQueue_.add(msg);
      }
      return null;
    }
  }

  private class AsyncTestCommOverlayCallable implements Callable<CommMessage> {
    private final CommMessage commMsg_;

    public AsyncTestCommOverlayCallable(final CommMessage commMsg) {
      commMsg_ = commMsg;
    }

    @Override
    public CommMessage call() throws Exception {
      final CommMessage checkMsg = new CheckCommunicationStateMessage(myAddress_,
          commMsg_.getDestinationAddress(), commMsg_.getId());

      communicationInQueue_.add(checkMsg);

      synchronized (waitingMessages_) {
        if (!waitingMessages_.containsKey(commMsg_.getDestinationAddress())) {
          waitingMessages_.put(commMsg_.getDestinationAddress(), new LinkedList<CommMessage>());
        }
        waitingMessages_.get(commMsg_.getDestinationAddress()).add(commMsg_);
      }
      return commMsg_;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    logger_.info("Communication overlay received a message: " + message);
    message.accept(visitor_);
  }
}
