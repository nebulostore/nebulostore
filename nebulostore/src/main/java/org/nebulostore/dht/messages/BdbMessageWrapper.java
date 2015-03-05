package org.nebulostore.dht.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author  Marcin Walas
 */
public class BdbMessageWrapper extends CommMessage {

  private static final long serialVersionUID = -7967599945241770910L;

  private final DHTMessage wrapped_;

  public BdbMessageWrapper(CommAddress sourceAddress, CommAddress destAddress,
      DHTMessage wrapped) {
    super(sourceAddress, destAddress);
    wrapped_ = wrapped;
  }

  public DHTMessage getWrapped() {
    return wrapped_;
  }
}
