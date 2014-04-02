package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;

/**
 * Service responsible for receiving byte arrays through network.
 *
 * For implementation details see {@link ByteSender}.
 *
 * @author Grzegorz Milka
 */
public interface ByteListenerService {
  void addObserver(Observer observer);

  void deleteObserver(Observer observer);

  BlockingQueue<byte[]> getListeningQueue();

  void startUp() throws IOException;

  void shutDown();
}
