package org.nebulostore.crypto.keys;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.dht.core.KeyDHT;

/**
 * @author lukaszsiczek
 */
public class DHTKeySource implements KeySource {

  private KeyDHT keyDHT_;
  private BlockingQueue<Message> dispatcherQueue_;

  public DHTKeySource(KeyDHT keyDHT, BlockingQueue<Message> dispatcherQueue) {
    keyDHT_ = keyDHT;
    dispatcherQueue_ = dispatcherQueue;
  }

  @Override
  public KeyHandler getKeyHandler() {
    return new DHTKeyHandler(keyDHT_, dispatcherQueue_);
  }

}
