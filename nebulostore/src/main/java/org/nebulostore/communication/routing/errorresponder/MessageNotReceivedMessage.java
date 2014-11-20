package org.nebulostore.communication.routing.errorresponder;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message sent to the module after error in sending originalMessage_.
 *
 * @author Piotr Malicki
 */
public class MessageNotReceivedMessage extends Message {

  private static final long serialVersionUID = 893858589297014667L;
  /**
   * Message that couldn't be delivered.
   */
  private final Message originalMessage_;

  public MessageNotReceivedMessage(Message originalMessage) {
    originalMessage_ = originalMessage;
  }

  public Message getOriginalMessage() {
    return originalMessage_;
  }

}
