package org.nebulostore.benchmarks.labtest.messages;

import org.nebulostore.appcore.messaging.Message;

public class PerformRandomActionMessage extends Message {
  private static final long serialVersionUID = 4584274381398776029L;

  public PerformRandomActionMessage(String jobID) {
    super(jobID);
  }
}
