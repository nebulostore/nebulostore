package org.nebulostore.communication.naming;

import java.net.InetSocketAddress;

import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.naming.addressmap.AddressMap;
import org.nebulostore.communication.naming.addressmap.AddressMapFactory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Grzegorz Milka
 */
public class SimpleCommAddressResolverTest {
  private SimpleCommAddressResolver resolver_;
  private AddressMap addressMap_;

  @Before
  public void setUp() {
    addressMap_ = mock(AddressMap.class);
    AddressMapFactory addressMapFactory = mock(AddressMapFactory.class);
    when(addressMapFactory.getAddressMap()).thenReturn(addressMap_);
    resolver_ = new SimpleCommAddressResolver(addressMapFactory);
  }

  @Test
  public void shouldReturnCorrectNetAddress() throws Exception {
    InetSocketAddress expectedNetAddress = new InetSocketAddress(0);
    when(addressMap_.getAddress(any(CommAddress.class))).thenReturn(expectedNetAddress);

    assertEquals(expectedNetAddress, resolver_.resolve(new CommAddress(0, 0)));
  }

  @Test(expected = AddressNotPresentException.class)
  public void shouldThrowsAddressNotPresentException() throws Exception {
    resolver_.resolve(new CommAddress(0, 0));
  }
}
