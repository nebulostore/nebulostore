package org.nebulostore.broker.messages;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Reply to a contract offer.
 * @author Bolek Kulbabinski
 */
public class OfferReplyMessage extends BrokerMessage {
  private static final long serialVersionUID = -6854062479094405282L;
  private EncryptedObject encryptedContract_;
  private String sessionId_;
  private boolean result_;

  public OfferReplyMessage(String jobId, CommAddress destAddress,
      EncryptedObject encryptedContract, String sessionId, boolean result) {
    super(jobId, destAddress);
    encryptedContract_ = encryptedContract;
    sessionId_ = sessionId;
    result_ = result;
  }

  public boolean getResult() {
    return result_;
  }

  public String getSessionId() {
    return sessionId_;
  }

  public EncryptedObject getEncryptedContract() {
    return encryptedContract_;
  }
}
