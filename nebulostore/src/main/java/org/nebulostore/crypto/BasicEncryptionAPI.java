package org.nebulostore.crypto;

import java.io.Serializable;

import org.nebulostore.appcore.model.EncryptedObject;

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
  public void load(String keyId, String keyFilePath, KeyLocation location, KeyType keyType) {
  }

}
