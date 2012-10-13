package org.nebulostore.appcore;

/**
 * Message indicating that some time is up.
 * @author szymonmatejczyk
 */
public class TimeoutMessage extends Message {
  private static final long serialVersionUID = -8674965519068356105L;

  public TimeoutMessage(String jobID) {
    super(jobID);
  }


}