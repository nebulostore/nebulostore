package org.nebulostore.communication.naming.addressmap;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.netutils.remotemap.RemoteMap;

/**
 * {@link AddressMap} represented by in-memory map structure.
 *
 * @author Grzegorz Milka
 *
 */
public class AddressMapAdapter implements AddressMap {
  private final RemoteMap remoteMap_;

  public AddressMapAdapter(RemoteMap remoteMap) {
    remoteMap_ = remoteMap;
  }

  @Override
  public InetSocketAddress getAddress(CommAddress commAddress) throws IOException {
    return (InetSocketAddress) remoteMap_.get(0, commAddress.toString());
  }

  @Override
  public void putAddress(CommAddress commAddress, InetSocketAddress netAddress) throws IOException {
    remoteMap_.put(0, commAddress.toString(), netAddress);
  }
}
