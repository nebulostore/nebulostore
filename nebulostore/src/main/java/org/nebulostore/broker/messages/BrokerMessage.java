package org.nebulostore.broker.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Bolek Kulbabinski
 */
public abstract class BrokerMessage extends CommMessage {
  private static final long serialVersionUID = -4489045030999308048L;

  public BrokerMessage(String jobId, CommAddress destAddress) {
    super(jobId, null, destAddress);
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }
}
