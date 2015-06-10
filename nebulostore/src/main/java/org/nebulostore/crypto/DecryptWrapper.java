package org.nebulostore.crypto;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class DecryptWrapper {

  private enum DecryptMode {
    SYMMETRIC, ASYMMETRIC
  }

  private EncryptionAPI encryptionAPI_;
  private DecryptMode mode_;
  private SecretKey secretKey_;
  private String decryptKeyId_;

  public DecryptWrapper(EncryptionAPI encryptionAPI, SecretKey secretKey) {
    encryptionAPI_ = encryptionAPI;
    mode_ = DecryptMode.SYMMETRIC;
    secretKey_ = secretKey;
  }

  public DecryptWrapper(EncryptionAPI encryptionAPI, String decryptKeyId) {
    encryptionAPI_ = encryptionAPI;
    mode_ = DecryptMode.ASYMMETRIC;
    decryptKeyId_ = decryptKeyId;
  }

  public Object decrypt(EncryptedObject encryptedObject) throws CryptoException {
    switch (mode_) {
      case ASYMMETRIC:
        return encryptionAPI_.decrypt(encryptedObject, decryptKeyId_);
      case SYMMETRIC:
        return encryptionAPI_.decryptSymetric(encryptedObject, secretKey_);
      default:
        throw new CryptoException("Wrong encryption mode");
    }
  }
}
