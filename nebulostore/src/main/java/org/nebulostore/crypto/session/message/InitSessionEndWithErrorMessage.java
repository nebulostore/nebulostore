package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionEndWithErrorMessage extends Message {

  private static final long serialVersionUID = 6029715086748333997L;

  private String errorMessage_;
  private CommAddress peerAddress_;

  public InitSessionEndWithErrorMessage(String jobId, String errorMessage,
      CommAddress peerAddress) {
    super(jobId);
    errorMessage_ = errorMessage;
    peerAddress_ = peerAddress;
  }

  public String getErrorMessage() {
    return errorMessage_;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }
}
