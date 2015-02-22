package org.nebulostore.crypto.keys;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class DHTKeySource implements KeySource {

  private CommAddress peerAddress_;
  private BlockingQueue<Message> dispatcherQueue_;

  public DHTKeySource(CommAddress peerAddress, BlockingQueue<Message> dispatcherQueue) {
    peerAddress_ = peerAddress;
    dispatcherQueue_ = dispatcherQueue;
  }

  @Override
  public KeyHandler getKeyHandler() {
    return new DHTKeyHandler(peerAddress_, dispatcherQueue_);
  }

  @Override
  public String toString() {
    return String.format("DHTKeySource: peerAddress_ %s", peerAddress_);
  }

}
