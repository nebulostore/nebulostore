package org.nebulostore.dht.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * @author Marcin Walas
 */
public class ReconfigureDHTAckMessage extends Message {
  /**
   *
   */
  private static final long serialVersionUID = -7941538232588795699L;

  public ReconfigureDHTAckMessage(ReconfigureDHTMessage request) {
    super(request.getId());
  }

}
