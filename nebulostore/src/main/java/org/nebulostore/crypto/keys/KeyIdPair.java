package org.nebulostore.crypto.keys;

/**
 * @author lukaszsiczek
 */
public class KeyIdPair {

  private String publicKeyId_;
  private String privateKeyId_;

  public KeyIdPair(String publicKeyId, String privateKeyId) {
    publicKeyId_ = publicKeyId;
    privateKeyId_ = privateKeyId;
  }

  public String getPublicKeyId() {
    return publicKeyId_;
  }

  public String getPrivateKeyId() {
    return privateKeyId_;
  }

  @Override
  public String toString() {
    return "Public: " + publicKeyId_ + " Private: " + privateKeyId_;
  }
}
