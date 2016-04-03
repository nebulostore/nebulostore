package org.nebulostore.crypto.session.message;

import java.io.Serializable;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.SessionObject;

/**
 * @author lukaszsiczek
 */
public class DHFinishMessage extends Message {

  private static final long serialVersionUID = -4874767425008086012L;

  private SessionObject initSessionObject_;

  public DHFinishMessage(SessionObject initSessionObject) {
    super(initSessionObject.getJobId());
    initSessionObject_ = initSessionObject;
  }

  public Serializable getData() {
    return initSessionObject_.getData();
  }

  public CommAddress getPeerAddress() {
    return initSessionObject_.getPeerAddress();
  }

  public SecretKey getSessionKey() {
    return initSessionObject_.getSessionKey();
  }

  public String getSessionId() {
    return initSessionObject_.getSessionId();
  }

  public SessionObject getSessionObject() {
    return initSessionObject_;
  }

  @Override
  public String toString() {
    return "InitSessionEndMessage{ initSessionObject_='" +
        initSessionObject_ + "} " + super.toString();
  }
}
