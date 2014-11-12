package org.nebulostore.async.synchrogroup.messages;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message indicating that the sender added itself as a synchro-peer of the receiver.
 *
 * @author Piotr Malicki
 *
 */
public class AddedAsSynchroPeerMessage extends CommMessage {

  private static final long serialVersionUID = 5731428748073128373L;

  public AddedAsSynchroPeerMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

}
