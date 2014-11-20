package org.nebulostore.communication.routing.errorresponder;


/**
 * Error responder which does nothing.
 *
 * @author Piotr Malicki
 *
 */

public final class EmptyErrorResponder implements ErrorResponder {

  @Override
  public void handleError() {
  }

}
