package org.nebulostore.communication.routing.errorresponder;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.messages.CommMessage;

/**
 * Error responder which sends an error message to the module which
 * was trying to send the original message.
 *
 * @author Piotr Malicki
 *
 */
public final class ErrorMessageErrorResponder extends ErrorResponder {

  /**
   * Queue for error messages.
   */
  private final BlockingQueue<Message> outQueue_;
  /**
   * Original message which was not sent.
   */
  private final CommMessage message_;

  public ErrorMessageErrorResponder(BlockingQueue<Message> outQueue,
    CommMessage message) {
    outQueue_ = outQueue;
    message_ = message;

  }

  @Override
  public void run() {
    outQueue_.add(new SendErrorMessage(message_));
  }

}
