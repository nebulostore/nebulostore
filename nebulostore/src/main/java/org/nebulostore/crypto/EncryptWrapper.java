package org.nebulostore.crypto;

import java.io.Serializable;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class EncryptWrapper {

  private enum EncryptMode {
    SYMMETRIC, ASYMMETRIC
  }

  private EncryptionAPI encryptionAPI_;
  private EncryptMode mode_;
  private SecretKey secretKey_;
  private String encryptKeyId_;

  public EncryptWrapper(EncryptionAPI encryptionAPI, SecretKey secretKey) {
    encryptionAPI_ = encryptionAPI;
    mode_ = EncryptMode.SYMMETRIC;
    secretKey_ = secretKey;
  }

  public EncryptWrapper(EncryptionAPI encryptionAPI, String encryptKeyId) {
    encryptionAPI_ = encryptionAPI;
    mode_ = EncryptMode.ASYMMETRIC;
    encryptKeyId_ = encryptKeyId;
  }

  public EncryptedObject encrypt(Serializable data) throws CryptoException {
    switch (mode_) {
      case ASYMMETRIC:
        return encryptionAPI_.encrypt(data, encryptKeyId_);
      case SYMMETRIC:
        return encryptionAPI_.encryptSymetric(data, secretKey_);
      default:
        throw new CryptoException("Wrong encryption mode");
    }
  }
}
