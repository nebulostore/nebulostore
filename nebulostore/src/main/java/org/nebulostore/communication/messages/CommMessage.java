package org.nebulostore.communication.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Created as a base class for all messages that are being sent over communication layer.
 * @author  Marcin Walas
 */
public abstract class CommMessage extends Message {
  private static final long serialVersionUID = -8777470468391282076L;

  private CommAddress commSourceAddress_;
  private CommAddress commDestAddress_;

  public CommMessage(CommAddress sourceAddress, CommAddress destAddress) {
    commSourceAddress_ = sourceAddress;
    commDestAddress_ = destAddress;
  }

  public CommMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress) {
    super(jobId);
    commSourceAddress_ = sourceAddress;
    commDestAddress_ = destAddress;
  }

  public CommAddress getSourceAddress() {
    return commSourceAddress_;
  }

  public CommAddress getDestinationAddress() {
    return commDestAddress_;
  }

  public void setSourceAddress(CommAddress sourceAddress) {
    commSourceAddress_ = sourceAddress;
  }

  public void setDestinationAddress(CommAddress destAddress) {
    commDestAddress_ = destAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    CommMessage that = (CommMessage) o;

    if (commDestAddress_ != null ?
        !commDestAddress_.equals(that.commDestAddress_) : that.commDestAddress_ != null) {
      return false;
    }
    if (commSourceAddress_ != null ?
        !commSourceAddress_.equals(that.commSourceAddress_) : that.commSourceAddress_ != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (commSourceAddress_ != null ? commSourceAddress_.hashCode() : 0);
    result = 31 * result + (commDestAddress_ != null ? commDestAddress_.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "CommMessage{" +
        "commSourceAddress_=" + commSourceAddress_ +
        ", commDestAddress_=" + commDestAddress_ +
        "} " + super.toString();
  }
}
