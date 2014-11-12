package org.nebulostore.async.synchrogroup.messages;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.synchrogroup.RemoveFromSynchroPeerSetModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message instructing receiver to remove itself from synchro-peer set of sender.
 *
 * @author Piotr Malicki
 *
 */
public class RemoveFromSynchroPeerSetMessage extends CommMessage {

  private static final long serialVersionUID = -4495476630993664494L;

  public RemoveFromSynchroPeerSetMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

  @Override
  public JobModule getHandler() {
    return new RemoveFromSynchroPeerSetModule();
  }

}
