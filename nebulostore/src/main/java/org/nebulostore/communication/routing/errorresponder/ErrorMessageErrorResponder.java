package org.nebulostore.communication.routing.errorresponder;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;

/**
 * Error responder which sends an error message to the module which
 * was trying to send the original message.
 *
 * @author Piotr Malicki
 *
 */
public final class ErrorMessageErrorResponder implements ErrorResponder {

  /**
   * Queue for error messages.
   */
  private final BlockingQueue<Message> errorQueue_;

  private final Message originalMessage_;

  public ErrorMessageErrorResponder(BlockingQueue<Message> errorQueue,
      Message originalMessage) {
    errorQueue_ = errorQueue;
    originalMessage_ = originalMessage;
  }

  @Override
  public void handleError() {
    errorQueue_.add(new MessageNotReceivedMessage(originalMessage_));
  }

}
