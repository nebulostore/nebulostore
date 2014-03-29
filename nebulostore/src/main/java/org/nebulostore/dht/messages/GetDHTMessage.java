package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.dht.core.KeyDHT;

/**
 * @author marcin
 */
public class GetDHTMessage extends InDHTMessage {
  private static final long serialVersionUID = -6134658511663501107L;
  private final KeyDHT key_;

  public GetDHTMessage(String jobId, KeyDHT key) {
    super(jobId);
    key_ = key;
  }

  public KeyDHT getKey() {
    return key_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetDHTMessage)) {
      return false;
    }

    GetDHTMessage that = (GetDHTMessage) o;

    if (key_ != null ? !key_.equals(that.key_) : that.key_ != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return key_ != null ? key_.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "GetDHTMessage for key: " + getKey();
  }
}
