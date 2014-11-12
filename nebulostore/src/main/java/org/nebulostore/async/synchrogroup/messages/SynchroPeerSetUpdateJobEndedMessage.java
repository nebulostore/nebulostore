package org.nebulostore.async.synchrogroup.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message indicating that synchro peer set update job was ended by module with job id set to
 * jobId_.
 *
 * @author Piotr Malicki
 *
 */
public class SynchroPeerSetUpdateJobEndedMessage extends Message {

  private static final long serialVersionUID = -703245486756109496L;

  public SynchroPeerSetUpdateJobEndedMessage(String jobId) {
    super(jobId);
  }

}
