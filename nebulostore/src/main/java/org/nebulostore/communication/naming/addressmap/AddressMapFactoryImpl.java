package org.nebulostore.communication.naming.addressmap;

import com.google.inject.Inject;

import org.nebulostore.communication.netutils.remotemap.RemoteMapFactory;

/**
 * @author Grzegorz Milka
 */
public class AddressMapFactoryImpl implements AddressMapFactory {
  private final AddressMap addressMap_;

  @Inject
  public AddressMapFactoryImpl(RemoteMapFactory remoteMapFactory) {
    addressMap_ = new AddressMapAdapter(remoteMapFactory.getRemoteMap());
  }

  @Override
  public AddressMap getAddressMap() {
    return addressMap_;
  }
}
