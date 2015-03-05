package org.nebulostore.conductor.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author szymonmatejczyk
 */
public class GatherStatsMessage extends CommMessage {
  private static final long serialVersionUID = -7578909223429521722L;

  public GatherStatsMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }
}
