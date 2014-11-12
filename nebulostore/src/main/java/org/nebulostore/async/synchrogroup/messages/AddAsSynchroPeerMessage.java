package org.nebulostore.async.synchrogroup.messages;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.synchrogroup.AddAsSynchroPeerModule;
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

  private final int counterValue_;

  public AddAsSynchroPeerMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress,
      int counterValue) {
    super(jobId, sourceAddress, destAddress);
    counterValue_ = counterValue;
  }

  public int getCounterValue() {
    return counterValue_;
  }

  @Override
  public JobModule getHandler() {
    return new AddAsSynchroPeerModule();
  }

}
