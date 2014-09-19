package org.nebulostore.async.messages;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Collection of messages sent for recipient_.
 * @author szymonmatejczyk
 */
public class AsynchronousMessagesMessage extends CommMessage {
  private static final long serialVersionUID = -2023624456880608658L;

  private final List<AsynchronousMessage> messages_;
  private final CommAddress recipient_;

  public AsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, List<AsynchronousMessage> messages, CommAddress recipient) {
    super(jobId, sourceAddress, destAddress);
    messages_ = messages;
    recipient_ = recipient;
  }

  public List<AsynchronousMessage> getMessages() {
    return messages_;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

}
