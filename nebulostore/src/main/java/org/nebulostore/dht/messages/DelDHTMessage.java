package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.dht.core.KeyDHT;

/**
 * @author marcin
 */
public class DelDHTMessage extends InDHTMessage {
  private static final long serialVersionUID = -3611478669143150333L;
  private final KeyDHT key_;

  public DelDHTMessage(String jobId, KeyDHT key) {
    super(jobId);
    key_ = key;
  }

  public KeyDHT getKey() {
    return key_;
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
