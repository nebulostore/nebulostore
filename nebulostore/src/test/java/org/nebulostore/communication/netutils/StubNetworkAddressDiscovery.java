package org.nebulostore.communication.netutils;

import java.net.InetSocketAddress;

/**
 * Stub {@link NetworkAddressDiscovery} which allows for manual set up and change of network
 * address.
 *
 * @author Grzegorz Milka
 *
 */
public class StubNetworkAddressDiscovery extends NetworkAddressDiscovery {
  private InetSocketAddress networkAddress_;

  public StubNetworkAddressDiscovery() {
  }

  public StubNetworkAddressDiscovery(InetSocketAddress networkAddress) {
    networkAddress_ = networkAddress;
  }

  @Override
  public InetSocketAddress getNetworkAddress() {
    return networkAddress_;
  }

  public void setNetworkAddress(InetSocketAddress newAddress) {
    networkAddress_ = newAddress;
    setChanged();
    notifyObservers(networkAddress_);
  }
}
