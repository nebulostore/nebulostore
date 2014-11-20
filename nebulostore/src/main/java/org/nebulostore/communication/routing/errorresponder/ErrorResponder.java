package org.nebulostore.communication.routing.errorresponder;


/**
 * Runnable object which is called in case of an error while sending a message.
 *
 * @author Piotr Malicki
 */

public interface ErrorResponder {

  void handleError();

}
