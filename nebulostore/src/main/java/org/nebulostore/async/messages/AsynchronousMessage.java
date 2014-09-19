package org.nebulostore.async.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Asynchronous message sent by peers when one of them is offline.
 * @author szymonmatejczyk
 */
public abstract class AsynchronousMessage extends Message {
  private static final long serialVersionUID = -8951534647349943846L;

  String id_;

  public AsynchronousMessage() {
    id_ = CryptoUtils.getRandomId().toString();
  }

  public AsynchronousMessage(String id) {
    super();
    id_ = id;
  }

  public String getMessageId() {
    return id_;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 37 * result + ((id_ == null) ? 0 : id_.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    AsynchronousMessage other = (AsynchronousMessage) obj;
    if (id_ == null) {
      if (other.id_ != null) {
        return false;
      }
    } else if (!id_.equals(other.id_)) {
      return false;
    }
    return true;
  }
}
