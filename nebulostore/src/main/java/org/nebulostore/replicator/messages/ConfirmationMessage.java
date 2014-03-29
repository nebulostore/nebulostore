package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send between Replicator and Network modules to indicate that object was
 * successfully stored.
 * @author szymonmatejczyk
 */
public class ConfirmationMessage extends OutReplicatorMessage {
  private static final long serialVersionUID = 4963514627405781252L;

  public ConfirmationMessage(String jobId, CommAddress destAddress) {
    super(jobId, destAddress);
  }
}
