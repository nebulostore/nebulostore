package org.nebulostore.crypto.keys;

import java.security.Key;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.api.GetKeyModule;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.PublicKeyMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author lukaszsiczek
 */
public class DHTKeyHandler implements KeyHandler {

  private static final int TIMEOUT_SEC = 30;
  private KeyDHT keyDHT_;
  private BlockingQueue<Message> dispatcherQueue_;

  public DHTKeyHandler(KeyDHT keyDHT, BlockingQueue<Message> dispatcherQueue) {
    keyDHT_ = keyDHT;
    dispatcherQueue_ = dispatcherQueue;
  }

  @Override
  public Key load() throws CryptoException {
    try {
      GetKeyModule getKeyModule = new GetKeyModule(dispatcherQueue_, keyDHT_);
      PublicKeyMetadata publicKeyMetadata =
        (PublicKeyMetadata) getKeyModule.getResult(TIMEOUT_SEC).getValue();
      Key key = publicKeyMetadata.getPublicKey();
      if (key == null) {
        throw new CryptoException("Unable to get Key from DHT");
      }
      return key;
    } catch (NebuloException e) {
      throw new CryptoException("Unable to get Key from DHT because of " + e.getMessage(), e);
    }
  }

  public void save(Key key) throws CryptoException {
    try {
      InstanceMetadata instanceMetadata = new InstanceMetadata();
      instanceMetadata.setInstancePublicKey(key);
      PutKeyModule putKeyModule = new PutKeyModule(dispatcherQueue_,
          keyDHT_, new ValueDHT(instanceMetadata));
      putKeyModule.getResult(TIMEOUT_SEC);
    } catch (NebuloException e) {
      throw new CryptoException("Unable to put metadata into DHT because of " + e.getMessage(), e);
    }
  }

}
