package pl.edu.uw.mimuw.nebulostore.communication.messages;

import pl.edu.uw.mimuw.nebulostore.communication.address.CommAddress;

/**
 * @author Marcin Walas
 */
public class MsgCommPeerFound extends CommMessage {

  public MsgCommPeerFound(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);

  }
}
