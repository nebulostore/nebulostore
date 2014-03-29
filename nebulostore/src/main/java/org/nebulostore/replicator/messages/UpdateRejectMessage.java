package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send to abort updating file.
 * @author szymonmatejczyk
 */
public class UpdateRejectMessage extends OutReplicatorMessage {
  private static final long serialVersionUID = 1010496789539196274L;

  public UpdateRejectMessage(String jobId, CommAddress destAddress) {
    super(jobId, destAddress);
  }
}
