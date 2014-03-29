package org.nebulostore.replicator.messages;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Replicator's responses.
 *
 * @author Bolek Kulbabinski
 */
public class OutReplicatorMessage extends ReplicatorMessage {
  private static final long serialVersionUID = -6423735953388543492L;

  public OutReplicatorMessage(CommAddress destAddress) {
    super(destAddress);
  }

  public OutReplicatorMessage(String jobId, CommAddress destAddress) {
    super(jobId, destAddress);
  }
}
