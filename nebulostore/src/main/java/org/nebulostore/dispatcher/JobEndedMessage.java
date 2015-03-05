package org.nebulostore.dispatcher;

import org.nebulostore.appcore.messaging.Message;

/**
 * Worker thread sends this message to dispatcher before it dies to
 * indicate that the task has ended and can be removed from the thread map.
 *
 * IMPORTANT: Be very careful when sending this message to dispatcher manually as it may cause
 * a significant delay when used incorrectly.
 * The only method that can safely produce this message is JobModule.endJobModule().
 */
public class JobEndedMessage extends Message {
  private static final long serialVersionUID = 575456984015987666L;

  public JobEndedMessage(String jobID) {
    super(jobID);
  }

}
