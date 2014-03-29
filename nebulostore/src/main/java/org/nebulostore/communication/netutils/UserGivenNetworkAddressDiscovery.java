package org.nebulostore.communication.netutils;

import java.net.InetSocketAddress;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * NetworkAddressDiscovery in which external network address is defined by user on start up.
 *
 * @author Grzegorz Milka
 *
 */
public class UserGivenNetworkAddressDiscovery extends NetworkAddressDiscovery {
  private final InetSocketAddress netAddress_;

  @Inject
  public UserGivenNetworkAddressDiscovery(
      @Named("communication.local-netsocket-address")InetSocketAddress netAddress) {
    netAddress_ = netAddress;
  }

  @Override
  public InetSocketAddress getNetworkAddress() {
    return netAddress_;
  }
}
