package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send to replicator by owner of an object, when he stores outdated version of object.
 *
 * Replicator should download the object again from another replica.
 */
public class ObjectOutdatedMessage extends InReplicatorMessage {
  private static final long serialVersionUID = -3210318617364486510L;

  private final NebuloAddress address_;

  public ObjectOutdatedMessage(CommAddress destAddress, NebuloAddress address) {
    super(destAddress);
    address_ = address;
  }

  public NebuloAddress getAddress() {
    return address_;
  }
}
