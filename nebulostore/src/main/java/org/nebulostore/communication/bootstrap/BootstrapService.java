package org.nebulostore.communication.bootstrap;

import java.io.IOException;

/**
 * Class responsible for providing bootstrap service to the network and host.
 *
 * @author Grzegorz Milka
 *
 */
public interface BootstrapService {
  BootstrapInformation getBootstrapInformation();

  void shutDown() throws InterruptedException;
  void startUp() throws IOException;
}
