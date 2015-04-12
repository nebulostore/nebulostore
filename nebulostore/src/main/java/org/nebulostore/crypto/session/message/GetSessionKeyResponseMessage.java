package org.nebulostore.crypto.session.message;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

public class GetSessionKeyResponseMessage extends Message {

  private static final long serialVersionUID = -7314530957894008209L;

  private CommAddress peerAddress_;
  private SecretKey sessionKey_;
  private String sessionId_;

  public GetSessionKeyResponseMessage(String jobId, CommAddress peerAddress,
      SecretKey sessionKey, String sessionId) {
    super(jobId);
    peerAddress_ = peerAddress;
    sessionKey_ = sessionKey;
    sessionId_ = sessionId;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public SecretKey getSessionKey() {
    return sessionKey_;
  }

  public String getSessionId() {
    return sessionId_;
  }
}
