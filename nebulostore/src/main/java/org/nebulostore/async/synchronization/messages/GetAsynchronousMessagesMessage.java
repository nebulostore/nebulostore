package org.nebulostore.async.synchronization.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.synchronization.RespondWithAsynchronousMessagesModule;
import org.nebulostore.async.synchronization.VectorClockValue;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message sent when peer wants to exchange asynchronous messages data for recipient_ with another
 * synchro-peer from recipient_'s synchro-peer set.
 */
public class GetAsynchronousMessagesMessage extends CommMessage {
  private static final long serialVersionUID = 132756955341183967L;

  private final CommAddress recipient_;
  /**
   * Vector clock value of sender for recipient_'s synchro group at the
   * moment of creating the message.
   */
  private final VectorClockValue timestamp_;


  public GetAsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, CommAddress recipient, VectorClockValue timestamp) {
    super(jobId, sourceAddress, destAddress);
    recipient_ = recipient;
    timestamp_ = timestamp;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  public VectorClockValue getTimestamp() {
    return timestamp_;
  }

  @Override
  public JobModule getHandler() {
    return new RespondWithAsynchronousMessagesModule();
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
