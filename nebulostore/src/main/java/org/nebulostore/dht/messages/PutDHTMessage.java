package org.nebulostore.dht.messages;

import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author marcin
 */
public class PutDHTMessage extends InDHTMessage {
  private static final long serialVersionUID = -2291359452486626170L;
  private final KeyDHT key_;
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

  @Override
  public String toString() {
    return "PutDHTMessage for (key, value): (" + getKey() + ", " +
          getValue() + ")";
  }
}
