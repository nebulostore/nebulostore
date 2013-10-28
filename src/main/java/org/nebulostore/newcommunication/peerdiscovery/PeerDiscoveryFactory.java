package org.nebulostore.newcommunication.peerdiscovery;

import java.util.Collection;

import com.google.inject.assistedinject.Assisted;

import org.nebulostore.communication.address.CommAddress;

/**
 * @author Grzegorz Milka
 */
public interface PeerDiscoveryFactory {
  PeerDiscovery newPeerDiscovery(
      @Assisted("communication.bootstrap-comm-addresses")
        Collection<CommAddress> bootstrapCommAddresses);
}
