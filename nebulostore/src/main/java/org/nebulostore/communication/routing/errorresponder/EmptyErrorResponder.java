package org.nebulostore.communication.routing.errorresponder;

import org.nebulostore.communication.routing.SendResult;

/**
 * Error responder which does nothing.
 *
 * @author Piotr Malicki
 *
 */

public final class EmptyErrorResponder extends ErrorResponder {

  @Override
  public void handleError(SendResult result) {
  }

}
