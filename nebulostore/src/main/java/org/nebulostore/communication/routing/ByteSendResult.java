package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.concurrent.CancellationException;

/**
 * Result of byte send operation.
 *
 * @author Grzegorz Milka
 */
public class ByteSendResult {
  private final CancellationException caE_;
  private final InterruptedException inE_;
  private final IOException ioE_;
  private final SendOperationIdentifier id_;

  public ByteSendResult(SendOperationIdentifier id) {
    id_ = id;
    caE_ = null;
    inE_ = null;
    ioE_ = null;
  }

  public ByteSendResult(SendOperationIdentifier id, CancellationException caE) {
    id_ = id;
    caE_ = caE;
    inE_ = null;
    ioE_ = null;
  }

  public ByteSendResult(SendOperationIdentifier id, InterruptedException inE) {
    id_ = id;
    caE_ = null;
    inE_ = inE;
    ioE_ = null;
  }

  public ByteSendResult(SendOperationIdentifier id, IOException ioE) {
    id_ = id;
    caE_ = null;
    inE_ = null;
    ioE_ = ioE;
  }

  public SendOperationIdentifier getId() {
    return id_;
  }

  /**
   * @return true iff successful
   * @throws throws exception iff send operation wasn't successful
   */
  public boolean getResult() throws InterruptedException, IOException {
    if (caE_ != null) {
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
