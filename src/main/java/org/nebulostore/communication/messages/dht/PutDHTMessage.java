package org.nebulostore.communication.messages.dht;

import org.nebulostore.appcore.MessageVisitor;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.dht.KeyDHT;
import org.nebulostore.communication.dht.ValueDHT;

/**
 * @author Marcin Walas
 */
public class PutDHTMessage extends InDHTMessage {
  /**
   */
  private final KeyDHT key_;
  /**
   */
  private final ValueDHT value_;

  public PutDHTMessage(String jobId, KeyDHT key, ValueDHT value) {
    super(jobId);
    key_ = key;
    value_ = value;
  }

  public KeyDHT getKey() {
    return key_;
  }

  public ValueDHT getValue() {
    return value_;
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "PutDHTMessage for (key, value): (" + getKey() + ", " +
          getValue() + ")";
  }
}
