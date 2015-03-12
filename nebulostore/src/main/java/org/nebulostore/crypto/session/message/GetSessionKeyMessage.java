package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public abstract class GetSessionKeyMessage extends Message {

  private static final long serialVersionUID = -7818319991343348884L;

  private CommAddress peerAddress_;
  private String sourceJobId_;

  public GetSessionKeyMessage(CommAddress peerAddress, String sourceJobId) {
    peerAddress_ = peerAddress;
    sourceJobId_ = sourceJobId;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public String getSourceJobId() {
    return sourceJobId_;
  }

  @Override
  public abstract JobModule getHandler();

}
