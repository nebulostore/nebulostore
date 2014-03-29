package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 * This message is send to idicate that execution of message send to the
 * Replicator encountered an error. The error message is written in message_.
 * @author szymonmatejczyk
 */
public class ReplicatorErrorMessage extends OutReplicatorMessage {
  private static final long serialVersionUID = -686759042653122970L;

  String message_;

  public ReplicatorErrorMessage(String jobId, CommAddress destinationAddress, String message) {
    super(jobId, destinationAddress);
    message_ = message;
  }

  public String getMessage() {
    return message_;
  }
}
