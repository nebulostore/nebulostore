package org.nebulostore.crypto;

import java.io.Serializable;

import javax.crypto.SecretKey;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.crypto.keys.KeySource;
import org.nebulostore.utils.Pair;

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
  public void load(String keyId, KeySource keySource, boolean saveInDHT) {
  }

  @Override
  public Pair<String, String> generatePublicPrivateKey() throws CryptoException {
    return new Pair<String, String>(null, null);
  }

}
