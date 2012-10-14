package org.nebulostore.communication.messages.dht;

import org.nebulostore.appcore.MessageVisitor;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * @author marcin
 */
public class ErrorDHTMessage extends OutDHTMessage {
  private static final long serialVersionUID = 5310737378968440051L;
  private final NebuloException exception_;

  public ErrorDHTMessage(InDHTMessage reqMessage, NebuloException exception) {
    super(reqMessage);
    exception_ = exception;
  }

  public NebuloException getException() {
    return exception_;
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
