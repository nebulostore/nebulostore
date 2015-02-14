package org.nebulostore.crypto;

import java.io.Serializable;

import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public abstract class EncryptionAPI {

  public enum KeyLocation {
    LOCAL_DISC, DHT
  }

  public enum KeyType {
    PUBLIC, PRIVATE
  }

  public abstract EncryptedObject encrypt(Serializable object, String keyId) throws CryptoException;

  public abstract Object decrypt(EncryptedObject cipher, String keyId) throws CryptoException;

  public abstract void load(String keyId, String keyFilePath, KeyLocation location,
      KeyType keyType) throws CryptoException;

}
