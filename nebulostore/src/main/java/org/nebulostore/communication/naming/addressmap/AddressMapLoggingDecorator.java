package org.nebulostore.communication.naming.addressmap;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Grzegorz Milka
 */
public class AddressMapLoggingDecorator implements AddressMap {
  private static final Logger LOGGER = Logger.getLogger(AddressMapLoggingDecorator.class);
  private final AddressMap addressMap_;

  public AddressMapLoggingDecorator(AddressMap addressMap) {
    addressMap_ = addressMap;
  }

  @Override
  public InetSocketAddress getAddress(CommAddress commAddress) throws IOException {
    InetSocketAddress netAddress = addressMap_.getAddress(commAddress);
    LOGGER.trace(String.format(
        "getAddress(%s): %s", commAddress.toString(),
        netAddress != null ? netAddress.toString() : null));
    return netAddress;
  }

  @Override
  public void putAddress(CommAddress commAddress, InetSocketAddress netAddress) throws IOException {
    LOGGER.trace(String.format("putAddress(%s, %s)", commAddress, netAddress));
    addressMap_.putAddress(commAddress, netAddress);
  }

}
