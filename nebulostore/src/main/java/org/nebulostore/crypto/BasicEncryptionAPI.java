package org.nebulostore.crypto;

import java.io.Serializable;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.crypto.keys.KeyHandler;

/**
 * @author lukaszsiczek
 */
public class BasicEncryptionAPI extends EncryptionAPI {

  @Override
  public EncryptedObject encrypt(Serializable object, String keyId) throws CryptoException {
    return new EncryptedObject(CryptoUtils.serializeObject(object));
  }

  @Override
  public Object decrypt(EncryptedObject cipher, String keyId) throws CryptoException {
    return CryptoUtils.deserializeObject(cipher.getEncryptedData());
  }

  @Override
  public EncryptedObject encryptSymetric(Serializable object, SecretKey key)
      throws CryptoException {
    return new EncryptedObject(CryptoUtils.serializeObject(object));
  }

  @Override
  public Object decryptSymetric(EncryptedObject cipher, SecretKey key) throws CryptoException {
    return CryptoUtils.deserializeObject(cipher.getEncryptedData());
  };

  @Override
  public EncryptedObject encryptWithSessionKey(Serializable object, SecretKey key)
      throws CryptoException {
    return new EncryptedObject(CryptoUtils.serializeObject(object));
  }

  @Override
  public Object decryptWithSessionKey(EncryptedObject cipher, SecretKey key)
      throws CryptoException {
    return CryptoUtils.deserializeObject(cipher.getEncryptedData());
  }

  @Override
  public void load(String keyId, KeyHandler keyHandler) {
  }

  @Override
  public String generateMAC(Serializable object, String keyId) throws CryptoException {
    return CryptoUtils.sha(encrypt(object, keyId));
  }

  @Override
  public boolean verifyMAC(Serializable object, String version, String keyId)
      throws CryptoException {
    return generateMAC(object, null).equals(version);
  }

  @Override
  public void remove(String keyId) {
  }

}
