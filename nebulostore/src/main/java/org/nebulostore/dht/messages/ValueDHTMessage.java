package org.nebulostore.dht.messages;

import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author marcin
 */
public class ValueDHTMessage extends OutDHTMessage {
  private static final long serialVersionUID = -4386207060074126596L;

  private final KeyDHT key_;
  private final ValueDHT value_;

  public ValueDHTMessage(InDHTMessage reqMessage, KeyDHT key, ValueDHT value) {
    super(reqMessage);
    key_ = key;
    value_ = value;
  }

  public KeyDHT getKey() {
    return key_;
  }

  public ValueDHT getValue() {
    return value_;
  }
}
