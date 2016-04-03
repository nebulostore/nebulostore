package org.nebulostore.crypto.keys;

import java.security.Key;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.api.GetKeyModule;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.PublicKeyMetadata;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author lukaszsiczek
 */
public class DHTKeyHandler implements KeyHandler {

  private static final int TIMEOUT_SEC = 30;
  private final KeyDHT keyDHT_;
  private final BlockingQueue<Message> dispatcherQueue_;
  private final InstanceMetadata instanceMetadata_;
  private final UserMetadata userMetadata_;
  private Key key_;

  public DHTKeyHandler(CommAddress instanceAddress, BlockingQueue<Message> dispatcherQueue) {
    keyDHT_ = instanceAddress.toKeyDHT();
    dispatcherQueue_ = dispatcherQueue;
    instanceMetadata_ = new InstanceMetadata();
    userMetadata_ = null;
  }

  public DHTKeyHandler(AppKey userAppKey, BlockingQueue<Message> dispatcherQueue) {
    keyDHT_ = new KeyDHT(userAppKey.getKey());
    dispatcherQueue_ = dispatcherQueue;
    instanceMetadata_ = null;
    userMetadata_ = new UserMetadata(userAppKey);
  }

  @Override
  public Key load() throws CryptoException {
    if (key_ != null) {
      return key_;
    }
    try {
      GetKeyModule getKeyModule = new GetKeyModule(dispatcherQueue_, keyDHT_);
      PublicKeyMetadata publicKeyMetadata =
        (PublicKeyMetadata) getKeyModule.getResult(TIMEOUT_SEC).getValue();
      key_ = publicKeyMetadata.getPublicKey();
      if (key_ == null) {
        throw new CryptoException("Unable to get Key from DHT");
      }
      return key_;
    } catch (NebuloException e) {
      throw new CryptoException("Unable to get Key from DHT because of " + e.getMessage(), e);
    }
  }

  public void save(Key key) throws CryptoException {
    try {
      PutKeyModule putKeyModule = null;
      if (instanceMetadata_ != null) {
        instanceMetadata_.setPublicKey(key);
        putKeyModule = new PutKeyModule(dispatcherQueue_,
            keyDHT_, new ValueDHT(instanceMetadata_));
      } else {
        userMetadata_.setPublicKey(key);
        putKeyModule = new PutKeyModule(dispatcherQueue_,
            keyDHT_, new ValueDHT(userMetadata_));
      }
      putKeyModule.getResult(TIMEOUT_SEC);
    } catch (NebuloException e) {
      throw new CryptoException("Unable to put metadata into DHT because of " + e.getMessage(), e);
    }
  }

}
