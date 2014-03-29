package org.nebulostore.dht.messages;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;

/**
 * @author marcin
 */
public abstract class InDHTMessage extends DHTMessage {
  private static final long serialVersionUID = -1471331171075924845L;
  private static Logger logger_ = Logger.getLogger(DHTMessage.class);

  public InDHTMessage(String id) {
    super(id);
    if (id == null) {
      logger_.warn("Empty id");
    }
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
