package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.replicator.core.TransactionAnswer;

/**
 * Sending transaction result to remote replicator.
 */
public class TransactionResultMessage extends InReplicatorMessage {
  private static final long serialVersionUID = 4182392038587696789L;

  private final TransactionAnswer result_;

  public TransactionResultMessage(String jobId, CommAddress destAddress, TransactionAnswer result) {
    super(jobId, destAddress);
    result_ = result;
  }

  public TransactionAnswer getResult() {
    return result_;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransactionResultMessage)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TransactionResultMessage that = (TransactionResultMessage) o;

    if (result_ != that.result_) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (result_ != null ? result_.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TransactionResultMessage{" +
        "result_=" + result_ +
        "} " + super.toString();
  }
}
