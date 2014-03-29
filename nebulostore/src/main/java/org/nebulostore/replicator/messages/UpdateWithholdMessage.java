package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Send as answer to StoreObjectMessage when object on this replica is not up to date.
 * @author szymonmatejczyk
 *
 */
public class UpdateWithholdMessage extends OutReplicatorMessage {

  private static final long serialVersionUID = 2939806231367561120L;

  /**
   * Reason for withholding transaction.
   */
  public static enum Reason { OBJECT_OUT_OF_DATE, SAVE_FAILURE, TIMEOUT }

  private final Reason reason_;


  public UpdateWithholdMessage(String jobId, CommAddress destAddress, Reason reason) {
    super(jobId, destAddress);
    reason_ = reason;
  }

  public Reason getReason() {
    return reason_;
  }

  @Override
  public String toString() {
    return "UpdateWithholdMessage{" +
        "reason_=" + reason_ +
        "} " + super.toString();
  }
}
