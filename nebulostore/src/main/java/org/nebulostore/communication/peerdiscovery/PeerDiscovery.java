package org.nebulostore.communication.peerdiscovery;

import java.util.Observer;

/**
 * Service which discovers new peers in the network.
 *
 * @author Grzegorz Milka
 */
public interface PeerDiscovery {
  /**
   * Adds observer which notifies when new peer has been found.
   *
   * As an object of notification it sends collection of newly found CommAddresses.
   * Newly found CommAddresses may not be unique.
   *
   * @param o
   */
  void addObserver(Observer o);
  void deleteObserver(Observer o);
  void startUp();
  void shutDown();
}
