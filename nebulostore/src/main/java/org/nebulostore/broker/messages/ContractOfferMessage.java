package org.nebulostore.broker.messages;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Broker's contract offer.
 * @author Bolek Kulbabinski
 */
public class ContractOfferMessage extends BrokerMessage {
  private static final long serialVersionUID = -578571854606199914L;
  private EncryptedObject encryptedContract_;
  private String sessionId_;

  public ContractOfferMessage(String jobId, CommAddress destAddress,
      EncryptedObject encryptedContract) {
    super(jobId, destAddress);
    encryptedContract_ = encryptedContract;
  }

  public ContractOfferMessage(String jobId, CommAddress destAddress, EncryptedObject offer,
      String sessionId) {
    this(jobId, destAddress, offer);
    sessionId_ = sessionId;
  }

  public EncryptedObject getEncryptedContract() {
    return encryptedContract_;
  }

  public String getSessionId() {
    return sessionId_;
  }

}
