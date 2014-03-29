package org.nebulostore.communication.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 *
 * @author Grzegorz Milka
 */
public class StubCommMessage extends CommMessage {
  private static final long serialVersionUID = 1L;
  private final String msg_;

  public StubCommMessage(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);
    msg_ = "";
  }

  public StubCommMessage(CommAddress sourceAddress, CommAddress destAddress, String msg) {
    super(sourceAddress, destAddress);
    msg_ = msg;
  }

  public String getMessage() {
    return msg_;
  }
}
