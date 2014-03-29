package org.nebulostore.broker.messages;

import org.nebulostore.broker.Contract;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Broker's contract offer.
 * @author Bolek Kulbabinski
 */
public class ContractOfferMessage extends BrokerMessage {
  private static final long serialVersionUID = -578571854606199914L;
  private Contract contract_;

  public ContractOfferMessage(String jobId, CommAddress destAddress, Contract contract) {
    super(jobId, destAddress);
    contract_ = contract;
  }

  public Contract getContract() {
    return contract_;
  }
}
