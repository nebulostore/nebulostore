package org.nebulostore.networkmonitor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Response for ConnectionTestMessage.
 */
public class ConnectionTestResponseMessage extends CommMessage {
  private static final long serialVersionUID = 1003452365644646925L;

  public ConnectionTestResponseMessage(String jobId, CommAddress destAddress) {
    super(jobId, null, destAddress);
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
