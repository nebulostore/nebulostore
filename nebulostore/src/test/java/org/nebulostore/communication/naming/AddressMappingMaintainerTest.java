package org.nebulostore.communication.naming;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.naming.addressmap.AddressMap;
import org.nebulostore.communication.naming.addressmap.AddressMapAdapter;
import org.nebulostore.communication.naming.addressmap.AddressMapFactory;
import org.nebulostore.communication.naming.addressmap.ObservableAddressMap;
import org.nebulostore.communication.naming.addressmap.ObservableAddressMap.PutEvent;
import org.nebulostore.communication.netutils.StubNetworkAddressDiscovery;
import org.nebulostore.communication.netutils.remotemap.RemoteMap;
import org.nebulostore.communication.netutils.remotemap.RemoteMapImpl;
import org.nebulostore.persistence.InMemoryStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Grzegorz Milka
 */
public class AddressMappingMaintainerTest {
  private static final CommAddress LOCAL_COMM_ADDRESS = new CommAddress(0, 0);
  private static final int CHECK_INTERVAL = 1000;
  private static final TimeUnit CHECK_INTERVAL_TU = TimeUnit.MILLISECONDS;
  private AddressMappingMaintainer amMaintainer_;

  private AddressMap addressMap_;
  private BlockingQueue<ObservableAddressMap.PutEvent> putEvents_;
  private StubNetworkAddressDiscovery addrDiscovery_;

  @Before
  public void setUp() {
    putEvents_ = new LinkedBlockingQueue<ObservableAddressMap.PutEvent>();
    AddressMapFactory addressMapFactory = mock(AddressMapFactory.class);
    RemoteMap remoteMap = new RemoteMapImpl(new InMemoryStore<Serializable>());
    addressMap_ = new ObservableAddressMap(new AddressMapAdapter(remoteMap), putEvents_);

    when(addressMapFactory.getAddressMap()).thenReturn(addressMap_);

    addrDiscovery_ = new StubNetworkAddressDiscovery();
    ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    amMaintainer_ = new AddressMappingMaintainer(
        addressMapFactory,
        addrDiscovery_,
        LOCAL_COMM_ADDRESS,
        scheduledExecutor,
        CHECK_INTERVAL,
        CHECK_INTERVAL_TU);
  }

  @Test(timeout = 2000)
  public void shouldCorrectIncorrectAddressMapEntry() throws Exception {
    InetSocketAddress incorrectNetAddr = new InetSocketAddress(0);
    InetSocketAddress correctNetAddr = new InetSocketAddress(1);

    addressMap_.putAddress(LOCAL_COMM_ADDRESS, incorrectNetAddr);
    addrDiscovery_.setNetworkAddress(correctNetAddr);
    amMaintainer_.startUp();

    while (true) {
      PutEvent event = putEvents_.take();
      if (event.getNetAddress().equals(correctNetAddr)) {
        break;
      }
    }

    amMaintainer_.shutDown();
  }

  @Test(timeout = 50000)
  public void shouldUpdateMapEntryOnAddressChange() throws Exception {
    InetSocketAddress earlierNetAddr = new InetSocketAddress(0);
    InetSocketAddress laterNetAddr = new InetSocketAddress(1);

    addrDiscovery_.setNetworkAddress(earlierNetAddr);
    amMaintainer_.startUp();

    while (true) {
      PutEvent event = putEvents_.take();
      if (event.getNetAddress().equals(earlierNetAddr)) {
        break;
      }
    }

    addrDiscovery_.setNetworkAddress(laterNetAddr);

    while (true) {
      PutEvent event = putEvents_.take();
      if (event.getNetAddress().equals(laterNetAddr)) {
        break;
      }
    }

    amMaintainer_.shutDown();
  }
}
