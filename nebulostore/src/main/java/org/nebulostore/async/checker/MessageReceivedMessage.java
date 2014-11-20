package org.nebulostore.async.checker;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message to the message receiving checker indicating that message with identifier
 * originalMessageId_ was successfully received.
 *
 * @author Piotr Malicki
 *
 */
public class MessageReceivedMessage extends CommMessage {

  private static final long serialVersionUID = 3207901521316111220L;

  private final String originalMessageId_;

  public MessageReceivedMessage(CommAddress sourceAddress, CommAddress destAddress,
      String originalMessageId) {
    super(sourceAddress, destAddress);
    originalMessageId_ = originalMessageId;
  }

  public String getOriginalMessageId() {
    return originalMessageId_;
  }
}
