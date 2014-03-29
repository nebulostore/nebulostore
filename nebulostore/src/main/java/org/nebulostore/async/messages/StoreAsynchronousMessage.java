package org.nebulostore.async.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.StoreAsynchronousMessagesModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Store asynchronous message.
 * @author szymonmatejczyk
 */
public class StoreAsynchronousMessage extends CommMessage {
  private static final long serialVersionUID = -491541878523453225L;
  CommAddress recipient_;
  AsynchronousMessage message_;

  public StoreAsynchronousMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, CommAddress recipient,
      AsynchronousMessage message) {
    super(jobId, sourceAddress, destAddress);
    recipient_ = recipient;
    message_ = message;
  }

  public AsynchronousMessage getMessage() {
    return message_;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new StoreAsynchronousMessagesModule();
  }
}
