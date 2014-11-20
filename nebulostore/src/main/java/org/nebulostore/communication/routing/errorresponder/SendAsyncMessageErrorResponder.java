package org.nebulostore.communication.routing.errorresponder;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.SendAsynchronousMessagesForPeerModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Error responder which sends asynchronous message to all synchro-peers
 * of the original recipient of the message.
 *
 * @author Piotr Malicki
 *
 */
public final class SendAsyncMessageErrorResponder implements ErrorResponder {

  private static Logger logger_ = Logger.getLogger(SendAsyncMessageErrorResponder.class);

  /**
   * Asynchronous message to be sent.
   */
  private final AsynchronousMessage asyncMsg_;
  /**
   * The original recipient of the message.
   */
  private final CommAddress recipient_;
  private final BlockingQueue<Message> dispatcherQueue_;

  public SendAsyncMessageErrorResponder(AsynchronousMessage asyncMsg, CommAddress recipient,
      BlockingQueue<Message> dispatcherQueue) {
    logger_.debug("Creating error responder, dispatcher queue: " + dispatcherQueue.hashCode());
    asyncMsg_ = asyncMsg;
    recipient_ = recipient;
    dispatcherQueue_ = dispatcherQueue;
  }

  @Override
  public void handleError() {
    logger_.debug("Sending asynchronous message: " + asyncMsg_);
    new SendAsynchronousMessagesForPeerModule(recipient_, asyncMsg_, dispatcherQueue_);
  }

}
