package org.nebulostore.communication.routing.errorresponder;

import java.io.IOException;
import java.util.Observable;
import java.util.Observer;

import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.routing.MessageSendFuture;
import org.nebulostore.communication.routing.SendResult;

/**
 * Runnable object which is called in case of an error while sending a message.
 *
 * @author Piotr Malicki
 */

public abstract class ErrorResponder implements Observer {

  @Override
  public void update(Observable o, Object arg) {
    MessageSendFuture future = (MessageSendFuture) o;
    try {
      future.get();
    } catch (AddressNotPresentException | InterruptedException | IOException e) {
      handleError(future.getResult());
    }
  }

  public abstract void handleError(SendResult result);

}
