package org.nebulostore.crypto;

import java.io.Serializable;
import java.security.Key;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.crypto.SecretKey;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.crypto.keys.KeyHandler;

/**
 * @author lukaszsiczek
 */
public class RSABasedEncryptionAPI extends EncryptionAPI {

  private static final Logger LOGGER = Logger.getLogger(RSABasedEncryptionAPI.class);

  private ConcurrentMap<String, KeyHandler> keys_;

  public RSABasedEncryptionAPI() {
    keys_ = new ConcurrentHashMap<String, KeyHandler>();
  }

  @Override
  public EncryptedObject encrypt(Serializable object, String keyId) throws CryptoException {
    LOGGER.debug(String.format("encrypt  %s", keyId));
    Key key = keys_.get(keyId).load();
    return CryptoUtils.encryptObject(object, key);
  }

  @Override
  public Object decrypt(EncryptedObject cipher, String keyId) throws CryptoException {
    LOGGER.debug(String.format("decrypt  %s", keyId));
    Key key = keys_.get(keyId).load();
    return CryptoUtils.decryptObject(cipher, key);
  }

  @Override
  public EncryptedObject encryptSymetric(Serializable object, SecretKey key)
      throws CryptoException {
    return CryptoUtils.encryptObjectWithSecretKey(object, key);
  };

  @Override
  public Object decryptSymetric(EncryptedObject cipher, SecretKey key) throws CryptoException {
    return CryptoUtils.decryptObjectWithSecretKey(cipher, key);
  };

  @Override
  public EncryptedObject encryptWithSessionKey(Serializable object, SecretKey key)
      throws CryptoException {
    LOGGER.debug("encrypt session key");
    return CryptoUtils.encryptObjectWithSessionKey(object, key);
  }

  @Override
  public Object decryptWithSessionKey(EncryptedObject cipher, SecretKey key)
      throws CryptoException {
    LOGGER.debug("decrypt session key");
    return CryptoUtils.decryptObjectWithSessionKey(cipher, key);
  }

  @Override
  public void load(String keyId, KeyHandler keyHandler) {
    keys_.put(keyId, keyHandler);
  };

}
