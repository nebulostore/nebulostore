package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class CheckContractResultMessage extends Message {

  private static final long serialVersionUID = 3738262379531314633L;

  private CommAddress peerAddress_;
  private String sessionId_;
  private boolean result_;

  public CheckContractResultMessage(String jobId, CommAddress peerAddress, String sessionId,
      boolean result) {
    super(jobId);
    peerAddress_ = peerAddress;
    sessionId_ = sessionId;
    result_ = result;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  public boolean getResult() {
    return result_;
  }

}
