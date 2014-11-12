package org.nebulostore.async.synchrogroup.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message with CommAddress of last peer found by the network monitor.
 *
 * @author Piotr Malicki
 *
 */
public class LastFoundPeerMessage extends Message {

  private static final long serialVersionUID = 5098091659815556562L;

  private final CommAddress lastPeer_;

  public LastFoundPeerMessage(String jobId, CommAddress lastPeer) {
    super(jobId);
    lastPeer_ = lastPeer;
  }

  public CommAddress getLastPeer() {
    return lastPeer_;
  }

}
