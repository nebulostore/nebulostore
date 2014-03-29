package org.nebulostore.communication.netutils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Observable;

/**
 * Objects of this class as responsible for finding out host's network IP/port address
 * and monitoring its change.
 *
 * This object is a runnable service that should be executed to work. It stops by sending interrupt
 * to its thread.
 *
 * It is observable and informs observers about change when new valid address is found.
 *
 * @author Grzegorz Milka
 *
 */
public abstract class NetworkAddressDiscovery extends Observable {
  /**
   * @return Current network address or null iff no valid address could be found.
   */
  public abstract InetSocketAddress getNetworkAddress();

  public void shutDown() throws InterruptedException {
  }

  public void startUp() throws IOException {
  }
}
