package org.nebulostore.async.messages;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AddAsSynchroPeerModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message instructing an instance to add itself as a synchro-peer of the sender.
 *
 * @author Piotr Malicki
 *
 */
public class AddAsSynchroPeerMessage extends CommMessage {

  private static final long serialVersionUID = 1L;

  public AddAsSynchroPeerMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

  @Override
  public JobModule getHandler() {
    return new AddAsSynchroPeerModule();
  }

}
