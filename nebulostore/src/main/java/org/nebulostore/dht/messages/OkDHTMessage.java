package org.nebulostore.dht.messages;

/**
 * @author marcin
 */
public class OkDHTMessage extends OutDHTMessage {
  private static final long serialVersionUID = -4259633350192458574L;

  public OkDHTMessage(InDHTMessage reqMessage) {
    super(reqMessage);
  }
}
