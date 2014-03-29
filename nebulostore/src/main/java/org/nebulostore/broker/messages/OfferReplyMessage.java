package org.nebulostore.broker.messages;

import org.nebulostore.broker.Contract;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Reply to a contract offer.
 * @author Bolek Kulbabinski
 */
public class OfferReplyMessage extends BrokerMessage {
  private static final long serialVersionUID = -6854062479094405282L;
  private Contract contract_;
  private boolean result_;

  public OfferReplyMessage(String jobId, CommAddress destAddress, Contract contract,
      boolean result) {
    super(jobId, destAddress);
    contract_ = contract;
    result_ = result;
  }

  public boolean getResult() {
    return result_;
  }

  public Contract getContract() {
    return contract_;
  }
}
