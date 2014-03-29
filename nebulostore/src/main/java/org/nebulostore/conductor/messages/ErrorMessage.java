package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send to indicate test error.
 * @author szymonmatejczyk
 *
 */
public class ErrorMessage extends CommMessage {
  private static final long serialVersionUID = -38582973328370010L;

  private final String message_;

  public ErrorMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, String message) {
    super(jobId, sourceAddress, destAddress);
    message_ = message;
  }

  public String getMessage() {
    return message_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
