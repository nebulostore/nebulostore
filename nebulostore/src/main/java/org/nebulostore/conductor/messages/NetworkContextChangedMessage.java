package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * Message send to indicate that NetworkContext has changed.
 * @author szymonmatejczyk
 *
 */
public class NetworkContextChangedMessage extends Message {
  private static final long serialVersionUID = -6053305223214969389L;

  public NetworkContextChangedMessage(String jobID) {
    super(jobID);
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
