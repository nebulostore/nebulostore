package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * @author Marcin Walas
 */
public class ReconfigureDHTAckMessage extends Message {
  /**
   *
   */
  private static final long serialVersionUID = -7941538232588795699L;

  public ReconfigureDHTAckMessage(ReconfigureDHTMessage request) {
    super(request.getId());
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
