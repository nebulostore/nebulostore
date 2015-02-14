package org.nebulostore.crypto.keys;

import java.security.Key;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.api.GetKeyModule;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author lukaszsiczek
 */
public class DHTKeyHandler implements KeyHandler {

  private static final int TIMEOUT_SEC = 30;
  private CommAddress peerAddress_;
  private BlockingQueue<Message> dispatcherQueue_;
  private AppKey appKey_;

  public DHTKeyHandler(CommAddress peerAddress,
      BlockingQueue<Message> dispatcherQueue, AppKey appKey) {
    peerAddress_ = peerAddress;
    dispatcherQueue_ = dispatcherQueue;
    appKey_ = appKey;
  }

  @Override
  public Key load() throws CryptoException {
    try {
      GetKeyModule getKeyModule = new GetKeyModule(dispatcherQueue_, peerAddress_.toKeyDHT());
      InstanceMetadata instanceMetadata =
        (InstanceMetadata) getKeyModule.getResult(TIMEOUT_SEC).getValue();
      return instanceMetadata.getPeerKey();
    } catch (NebuloException e) {
      throw new CryptoException("Unable to get instance metadata from DHT because of " +
          e.getMessage(), e);
    }
  }

  public void save(Key key) throws CryptoException {
    try {
      InstanceMetadata instanceMetadata = new InstanceMetadata(appKey_);
      instanceMetadata.setPeerKey(key);
      PutKeyModule putKeyModule = new PutKeyModule(dispatcherQueue_,
          peerAddress_.toKeyDHT(), new ValueDHT(instanceMetadata));
      putKeyModule.getResult(TIMEOUT_SEC);
    } catch (NebuloException e) {
      throw new CryptoException("Unable to put instance metadata into DHT because of " +
          e.getMessage(), e);
    }
  }

}
