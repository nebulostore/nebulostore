package org.nebulostore.async.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message that is sent to another peer in case of an error while handling a request for
 * asynchronous messages module.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncModuleErrorMessage extends CommMessage {

  private static final long serialVersionUID = 4883818720183897502L;

  public AsyncModuleErrorMessage(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);
  }

}
