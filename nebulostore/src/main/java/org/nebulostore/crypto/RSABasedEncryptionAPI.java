package org.nebulostore.crypto;

import java.io.Serializable;
import java.security.Key;
import java.security.KeyPair;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.keys.DHTKeyHandler;
import org.nebulostore.crypto.keys.FileKeySource;
import org.nebulostore.crypto.keys.KeyHandler;
import org.nebulostore.crypto.keys.KeySource;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public class RSABasedEncryptionAPI extends EncryptionAPI {

  private static final Logger LOGGER = Logger.getLogger(RSABasedEncryptionAPI.class);

  private ConcurrentMap<String, KeyHandler> keys_;
  private CommAddress peerAddress_;
  private BlockingQueue<Message> dispatcherQueue_;

  @Inject
  public RSABasedEncryptionAPI(CommAddress peerAddress,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    keys_ = new ConcurrentHashMap<String, KeyHandler>();
    peerAddress_ = peerAddress;
    dispatcherQueue_ = dispatcherQueue;
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
  public void load(String keyId, KeySource keySource, boolean saveInDHT) throws CryptoException {
    LOGGER.debug(String.format("load %s %s %s", keyId, keySource, saveInDHT));
    KeyHandler keyHandler = keySource.getKeyHandler();
    if (saveInDHT) {
      DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(peerAddress_.toKeyDHT(), dispatcherQueue_);
      dhtKeyHandler.save(keyHandler.load());
      keyHandler = dhtKeyHandler;
    }
    keys_.put(keyId, keyHandler);
  }

  /**
   * @return Pair<Private Key Id, Public Key Id>
   */
  @Override
  public Pair<String, String> generatePublicPrivateKey() throws CryptoException {
    LOGGER.debug(String.format("generatePublicPrivateKey"));
    KeyPair keyPair = CryptoUtils.generateKeyPair();
    String privateKeyId = CryptoUtils.getRandomString();
    String publicKeyId = CryptoUtils.getRandomString();
    String privateKeyPath = CryptoUtils.saveKeyOnDisk(keyPair.getPrivate(), privateKeyId);
    String publicKeyPath = CryptoUtils.saveKeyOnDisk(keyPair.getPublic(), publicKeyId);
    load(privateKeyId, new FileKeySource(privateKeyPath, KeyType.PRIVATE), !STORE_IN_DHT);
    load(publicKeyId, new FileKeySource(publicKeyPath, KeyType.PUBLIC), !STORE_IN_DHT);
    return new Pair<String, String>(privateKeyId, publicKeyId);
  }
}
