package org.nebulostore.communication.routing.errorresponder;

/**
 * Runnable object which is called in case of error while sending a message.
 *
 * @author Piotr Malicki
 */

public abstract class ErrorResponder implements Runnable {

  public boolean isQuickNonBlockingTask() {
    return true;
  }


}
