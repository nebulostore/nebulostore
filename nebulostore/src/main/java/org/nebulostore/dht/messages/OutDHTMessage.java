package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * @author marcin
 */
public abstract class OutDHTMessage extends DHTMessage {
  private static final long serialVersionUID = -6457851902716976285L;
  private final InDHTMessage requestMessage_;

  public OutDHTMessage(InDHTMessage reqMessage) {
    super(reqMessage.getId());
    requestMessage_ = reqMessage;
  }

  public InDHTMessage getRequestMessage() {
    return requestMessage_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
