package org.nebulostore.crypto;

import java.io.Serializable;
import java.security.Key;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.keys.DHTKeyHandler;
import org.nebulostore.crypto.keys.KeyHandler;
import org.nebulostore.crypto.keys.LocalDiscKeyHandler;

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
  public void load(String keyId, String keyFilePath, KeyLocation location,
      KeyType keyType) throws CryptoException {
    LOGGER.debug(String.format("load %s %s %s", keyId, keyFilePath, location));
    KeyHandler keyHandler = new LocalDiscKeyHandler(keyFilePath, keyType);
    switch (location) {
      case DHT:
        DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(peerAddress_, dispatcherQueue_);
        dhtKeyHandler.save(keyHandler.load());
        keys_.put(keyId, dhtKeyHandler);
        break;
      case LOCAL_DISC:
        keys_.put(keyId, keyHandler);
        break;
      default:
        break;
    }
  }

}
