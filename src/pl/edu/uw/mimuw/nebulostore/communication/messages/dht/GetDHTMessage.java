package pl.edu.uw.mimuw.nebulostore.communication.messages.dht;

import pl.edu.uw.mimuw.nebulostore.communication.dht.KeyDHT;
import pl.edu.uw.mimuw.nebulostore.communication.messages.CommMessage;

/**
 * @author marcin
 */
public class GetDHTMessage extends CommMessage {

  private final KeyDHT key_;

  public GetDHTMessage(KeyDHT key) {
    super(null, null);
    key_ = key;
  }
}
