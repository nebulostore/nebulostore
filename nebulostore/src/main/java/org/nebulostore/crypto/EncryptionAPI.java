package org.nebulostore.crypto;

import java.io.Serializable;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.crypto.keys.KeySource;

/**
 * @author lukaszsiczek
 */
public abstract class EncryptionAPI {

  public static final boolean STORE_IN_DHT = true;

  public enum KeyType {
    PUBLIC, PRIVATE
  }

  public abstract EncryptedObject encrypt(Serializable object, String keyId) throws CryptoException;

  public abstract Object decrypt(EncryptedObject cipher, String keyId) throws CryptoException;

  public abstract void load(String keyId, KeySource keySource,
      boolean saveInDHT) throws CryptoException;

}
