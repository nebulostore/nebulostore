package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
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

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
