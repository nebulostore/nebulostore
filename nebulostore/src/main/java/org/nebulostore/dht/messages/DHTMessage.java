package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * @author marcin
 */
public abstract class DHTMessage extends Message {
  private static final long serialVersionUID = 2403620888224729984L;

  public DHTMessage(String id) {
    super(id);
  }

  @Override
  public abstract <R> R accept(MessageVisitor<R> visitor) throws NebuloException;
}
