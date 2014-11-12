package org.nebulostore.async.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Asynchronous message sent by peers when one of them is offline.
 * @author szymonmatejczyk
 */
public abstract class AsynchronousMessage extends Message {
  private static final long serialVersionUID = -8951534647349943846L;

  @Override
  public String toString() {
    return "AsynchronousMessage {messageId: '" + id_ + "'}";
  }
}
