package org.nebulostore.crypto.session.message;

import java.io.Serializable;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionObject;

/**
 * @author lukaszsiczek
 */
public class InitSessionEndMessage extends Message {

  private static final long serialVersionUID = -4874767425008086012L;

  private InitSessionObject initSessionObject_;
  private String sourceJobId_;

  public InitSessionEndMessage(InitSessionObject initSessionObject, String sourceJobId) {
    super(initSessionObject.getJobId());
    initSessionObject_ = initSessionObject;
    sourceJobId_ = sourceJobId;
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

  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return initSessionObject_.getSessionId();
  }

  @Override
  public String toString() {
    return "InitSessionEndMessage{ initSessionObject_='" +
        initSessionObject_ + "} " + super.toString();
  }
}
