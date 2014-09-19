package org.nebulostore.systest.async.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

public class ResponseWithCommunicationStateMessage extends CommMessage {

  private static final long serialVersionUID = -6790464906058409015L;

  private final boolean isCommEnabled_;
  private final String originalMsgId_;

  public ResponseWithCommunicationStateMessage(CommAddress sourceAddress, CommAddress destAddress,
    boolean isCommEnabled, String originalMsgId) {
    super(sourceAddress, destAddress);
    isCommEnabled_ = isCommEnabled;
    originalMsgId_ = originalMsgId;
  }

  public boolean isCommEnabled() {
    return isCommEnabled_;
  }

  public String getOriginalMsgId() {
    return originalMsgId_;
  }

}
