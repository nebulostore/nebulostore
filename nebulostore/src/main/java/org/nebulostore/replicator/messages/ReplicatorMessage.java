package org.nebulostore.replicator.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Base class for all replicator messages.
 *
 * @see Replicator
 * @author Bolek Kulbabinski
 */
public abstract class ReplicatorMessage extends CommMessage {
  private static final long serialVersionUID = 2732807823967546590L;

  public ReplicatorMessage(CommAddress destAddress) {
    super(null, destAddress);
  }

  public ReplicatorMessage(String jobId, CommAddress destAddress) {
    super(jobId, null, destAddress);
  }
}
