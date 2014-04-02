package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.concurrent.CancellationException;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;

/**
 * Result of message send operation.
 *
 * @author Grzegorz Milka
 *
 */
public class SendResult {
  private final AddressNotPresentException anE_;
  private final CancellationException caE_;
  private final InterruptedException inE_;
  private final CommMessage msg_;
  private final IOException ioE_;

  public SendResult(CommMessage msg) {
    msg_ = msg;
    anE_ = null;
    caE_ = null;
    inE_ = null;
    ioE_ = null;
  }

  public SendResult(CommMessage msg, AddressNotPresentException anE) {
    msg_ = msg;
    anE_ = anE;
    caE_ = null;
    inE_ = null;
    ioE_ = null;
  }

  public SendResult(CommMessage msg, CancellationException caE) {
    msg_ = msg;
    anE_ = null;
    caE_ = caE;
    inE_ = null;
    ioE_ = null;
  }

  public SendResult(CommMessage msg, InterruptedException inE) {
    msg_ = msg;
    anE_ = null;
    caE_ = null;
    inE_ = inE;
    ioE_ = null;
  }

  public SendResult(CommMessage msg, IOException ioE) {
    msg_ = msg;
    anE_ = null;
    caE_ = null;
    inE_ = null;
    ioE_ = ioE;
  }

  public CommMessage getMsg() {
    return msg_;
  }

  /**
   * @return true iff successful
   * @throws throws exception iff send operation wasn't successful
   */
  public boolean getResult() throws AddressNotPresentException,
      InterruptedException, IOException {
    if (anE_ != null) {
      throw anE_;
    } else if (caE_ != null) {
      throw caE_;
    } else if (inE_ != null) {
      throw inE_;
    } else if (ioE_ != null) {
      throw ioE_;
    } else {
      return true;
    }
  }
}
