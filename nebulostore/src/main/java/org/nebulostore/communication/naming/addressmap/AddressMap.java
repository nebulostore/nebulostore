package org.nebulostore.communication.naming.addressmap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.Remote;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Map translating {@link CommAddress} names to IP addresses.
 *
 * @author Grzegorz Milka
 *
 */
public interface AddressMap extends Remote {
  /**
   * Gives current IP/port address for given CommAddress.
   *
   * @param commAddress
   * @return null if no mapping exists.
   * @throws IOException
   */
  InetSocketAddress getAddress(CommAddress commAddress) throws IOException;

  /**
   * Tries to put given mapping from CommAddress.
   *
   * Note only CommAddresses that are managed by calling host are allowed.
   *
   * @param commAddress
   * @param netAddress
   * @throws IOException
   */
  void putAddress(CommAddress commAddress, InetSocketAddress netAddress) throws IOException;
}
