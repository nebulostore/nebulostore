package org.nebulostore.async.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.ResponseWithAsynchronousMessagesModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send, when peer wants to receive messages waiting for him.
 */
public class GetAsynchronousMessagesMessage extends CommMessage {
  private static final long serialVersionUID = 132756955341183967L;

  CommAddress recipient_;

  public GetAsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, CommAddress recipient) {
    super(jobId, sourceAddress, destAddress);
    recipient_ = recipient;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  @Override
  public JobModule getHandler() {
    return new ResponseWithAsynchronousMessagesModule();
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
