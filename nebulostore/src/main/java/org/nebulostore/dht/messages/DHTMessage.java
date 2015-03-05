package org.nebulostore.dht.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * @author marcin
 */
public abstract class DHTMessage extends Message {
  private static final long serialVersionUID = 2403620888224729984L;

  public DHTMessage(String id) {
    super(id);
  }
}
