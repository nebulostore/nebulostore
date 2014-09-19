package org.nebulostore.systest.async.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

public class CheckCommunicationStateMessage extends CommMessage {

  private static final long serialVersionUID = -5145289158746878746L;

  private final String originalMessageId_;

  public CheckCommunicationStateMessage(CommAddress sourceAddress, CommAddress destAddress,
    String originalMessageId) {
    super(sourceAddress, destAddress);
    originalMessageId_ = originalMessageId;
  }

  public String getOriginalMessageId() {
    return originalMessageId_;
  }

}
