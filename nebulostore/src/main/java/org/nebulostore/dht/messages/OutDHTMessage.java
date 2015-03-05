package org.nebulostore.dht.messages;

/**
 * @author marcin
 */
public abstract class OutDHTMessage extends DHTMessage {
  private static final long serialVersionUID = -6457851902716976285L;
  private final InDHTMessage requestMessage_;

  public OutDHTMessage(InDHTMessage reqMessage) {
    super(reqMessage.getId());
    requestMessage_ = reqMessage;
  }

  public InDHTMessage getRequestMessage() {
    return requestMessage_;
  }

}
