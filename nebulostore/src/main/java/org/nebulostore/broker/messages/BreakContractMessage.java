package org.nebulostore.broker.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.broker.Contract;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send by another peer to acknowledge contract break.
 * @author szymon
 *
 */
public class BreakContractMessage extends CommMessage {

  private static final long serialVersionUID = -7437544862348721861L;

  private final Contract contract_;

  public BreakContractMessage(CommAddress sourceAddress, CommAddress destAddress,
      Contract contract) {
    super(sourceAddress, destAddress);
    contract_ = contract;
  }

  public Contract getContract() {
    return contract_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

}
