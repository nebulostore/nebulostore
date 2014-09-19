package org.nebulostore.communication.routing.errorresponder;

import java.util.Observable;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.communication.routing.MessageSendFuture;
import org.nebulostore.communication.routing.SendResult;

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
  private final BlockingQueue<SendResult> errorQueue_;

  public ErrorMessageErrorResponder(BlockingQueue<SendResult> outQueue) {
    errorQueue_ = outQueue;
  }

  @Override
  public void update(Observable o, Object arg) {
    MessageSendFuture future = (MessageSendFuture) o;
    SendResult result = future.getResult();
    errorQueue_.add(result);

  }

  @Override
  public void handleError(SendResult result) {
    errorQueue_.add(result);
  }

}
