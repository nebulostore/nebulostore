package org.nebulostore.crypto.session;

import java.io.Serializable;

import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionObject implements Serializable {

  private static final long serialVersionUID = 1199048874217118145L;

  private Serializable data_;
  private CommAddress peerAddress_;
  private String sourceJobId_;
  private transient KeyAgreement keyAgreement_;
  private SecretKey sessionKey_;

  public InitSessionObject(CommAddress peerAddress, String sourceJobId, Serializable data) {
    peerAddress_ = peerAddress;
    sourceJobId_ = sourceJobId;
    data_ = data;
  }

  public InitSessionObject(CommAddress peerAddress) {
    peerAddress_ = peerAddress;
  }

  public void setKeyAgreement(KeyAgreement keyAgreement) {
    keyAgreement_ = keyAgreement;
  }

  public void setSessionKey(SecretKey sessionKey) {
    sessionKey_ = sessionKey;
  }

  public KeyAgreement getKeyAgreement() {
    return keyAgreement_;
  }

  public Serializable getData() {
    return data_;
  }

  public String getJobId() {
    return sourceJobId_;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  public SecretKey getSessionKey() {
    return sessionKey_;
  }

  @Override
  public String toString() {
    return "InitSessionObject { peerAddress_: " + peerAddress_ + ", sessionKey_: " +
        sessionKey_.hashCode() + " }";
  }
}
