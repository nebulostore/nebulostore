package org.nebulostore.async.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * ACK message send by peer that succesfully received asynchronous messages.
 * @author szymonmatejczyk
 */
public class GotAsynchronousMessagesMessage extends CommMessage {
  CommAddress recipient_;

  public GotAsynchronousMessagesMessage(String jobId,
      CommAddress sourceAddress, CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

  private static final long serialVersionUID = 3450313559562082115L;

  public GotAsynchronousMessagesMessage(String jobId,
      CommAddress sourceAddress, CommAddress destAddress, CommAddress recipient) {
    super(jobId, sourceAddress, destAddress);
    recipient_ = recipient;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
