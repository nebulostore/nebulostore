package org.nebulostore.async.messages;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Collection of messages that are to be delivered.
 * @author szymonmatejczyk
 */
public class AsynchronousMessagesMessage extends CommMessage {
  private static final long serialVersionUID = -2023624456880608658L;

  private List<AsynchronousMessage> messages_;

  public AsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, List<AsynchronousMessage> messages) {
    super(jobId, sourceAddress, destAddress);
    messages_ = messages;
  }

  public List<AsynchronousMessage> getMessages() {
    return messages_;
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
