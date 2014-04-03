package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.address.CommAddress;
import org.nebulostore.communication.messages.CommMessage;

/**
 * Message send when TestModule has successfully finished his execution in current phase.
 * @author szymonmatejczyk
 */
public class TocMessage extends CommMessage {

  private static final long serialVersionUID = 8214780097258463061L;
  private final int phase_;

  public TocMessage(CommAddress sourceAddress, CommAddress destAddress, int phase) {
    super(sourceAddress, destAddress);
    phase_ = phase;
  }

  public TocMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, int phase) {
    super(jobId, sourceAddress, destAddress);
    phase_ = phase;
  }

  public int getPhase() {
    return phase_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "TocMessage{" +
        "phase_=" + phase_ +
        "} " + super.toString();
  }
}
