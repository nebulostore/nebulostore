package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * @author marcin
 */
public class OkDHTMessage extends OutDHTMessage {
  private static final long serialVersionUID = -4259633350192458574L;

  public OkDHTMessage(InDHTMessage reqMessage) {
    super(reqMessage);
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
