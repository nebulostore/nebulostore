package org.nebulostore.communication.naming.addressmap;

/**
 * Factory for producing AddressMap clients and also maintaining some properties of address map.
 *
 * @author Grzegorz Milka
 */
public interface AddressMapFactory {
  AddressMap getAddressMap();
}
