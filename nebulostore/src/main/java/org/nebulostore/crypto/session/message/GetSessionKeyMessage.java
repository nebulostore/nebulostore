package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorModule;

/**
 * @author lukaszsiczek
 */
public class GetSessionKeyMessage extends Message {

  private static final long serialVersionUID = -7818319991343348884L;

  private CommAddress peerAddress_;
  private String sourceJobId_;
  private String sessionId_;

  public GetSessionKeyMessage(CommAddress peerAddress, String sourceJobId, String sessionId) {
    peerAddress_ = peerAddress;
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorModule();
  }

}
