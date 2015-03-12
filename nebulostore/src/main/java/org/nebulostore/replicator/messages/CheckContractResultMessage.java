package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class CheckContractResultMessage extends Message {

  private static final long serialVersionUID = 3738262379531314633L;

  private CommAddress peerAddress_;
  private boolean result_;

  public CheckContractResultMessage(String jobId, CommAddress peerAddress, boolean result) {
    super(jobId);
    peerAddress_ = peerAddress;
    result_ = result;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public boolean getResult() {
    return result_;
  }

}
