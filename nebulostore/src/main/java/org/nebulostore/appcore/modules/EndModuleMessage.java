package org.nebulostore.appcore.modules;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * Command to end (terminate) module.
 * @author Grzegorz Milka
 */
public class EndModuleMessage extends Message {
  private static final long serialVersionUID = -1756129179954128771L;
  
  public EndModuleMessage() {
    super();
  }

  public EndModuleMessage(String jobId) {
    super(jobId);
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
