package org.nebulostore.broker.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.replicator.messages.CheckContractResultMessage;

/**
 * @author lukaszsiczek
 */
public class CheckContractMessage extends Message {

  private static final long serialVersionUID = 1467672738126331489L;

  private String sourceJobId_;
  private CommAddress peerAddress_;
  private String sessionId_;

  public CheckContractMessage(String sourceJobId, CommAddress peerAddress, String sessionId) {
    sourceJobId_ = sourceJobId;
    peerAddress_ = peerAddress;
    sessionId_ = sessionId;
  }

  public CommAddress getContractPeer() {
    return peerAddress_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }

  public CheckContractResultMessage getResponse(boolean result) {
    return new CheckContractResultMessage(sourceJobId_, peerAddress_, sessionId_, result);
  }
}
