package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send to set up a test on a client.
 * @author szymonmatejczyk
 *
 */
public class InitMessage extends CommMessage {
  private static final long serialVersionUID = 2556576233416223608L;

  private final JobModule handler_;

  public InitMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, JobModule handler) {
    super(jobId, sourceAddress, destAddress);
    handler_ = handler;
  }

  @Override
  public JobModule getHandler() {
    return handler_;
  }
}
