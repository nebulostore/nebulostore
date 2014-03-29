package org.nebulostore.dht.messages;

import java.util.Arrays;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Wrapper for packets sent over network by Angus implementation
 * of Kademlia protocol.
 *
 * @author Marcin Walas *
 */
public class KademliaMessage extends CommMessage {
  private static final long serialVersionUID = -7308102705214338084L;
  private final byte[] data_;

  public KademliaMessage(CommAddress sourceAddress, CommAddress destAddress,
      byte[] data) {
    super(sourceAddress, destAddress);
    data_ = Arrays.copyOf(data, data.length);
  }

  public byte[] getData() {
    return Arrays.copyOf(data_, data_.length);
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
