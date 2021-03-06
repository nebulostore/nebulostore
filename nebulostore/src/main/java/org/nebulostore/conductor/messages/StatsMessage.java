package org.nebulostore.conductor.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;

/**
 * @author szymonmatejczyk
 */
public class StatsMessage extends CommMessage {
  private static final long serialVersionUID = 2946080833935513302L;

  private final CaseStatistics stats_;

  public StatsMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, CaseStatistics stats) {
    super(jobId, sourceAddress, destAddress);
    stats_ = stats;
  }

  public CaseStatistics getStats() {
    return stats_;
  }

}
