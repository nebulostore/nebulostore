package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.communication.messages.CommMessage;

/**
 * Service responsible for receiving CommMessages through network.
 *
 * @author Grzegorz Milka
 */
public interface ListenerService {
  BlockingQueue<CommMessage> getListeningQueue();

  void startUp() throws IOException;

  void shutDown();
}
