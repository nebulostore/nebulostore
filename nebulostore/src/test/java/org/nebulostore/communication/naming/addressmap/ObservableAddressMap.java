package org.nebulostore.communication.naming.addressmap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Decorator for AddressMap which informs about all put requests through {@link BlockingQueue}.
 *
 * @author Grzegorz Milka
 */
public class ObservableAddressMap implements AddressMap {
  private final AddressMap addressMap_;
  private final BlockingQueue<PutEvent> putEvents_;

  public ObservableAddressMap(AddressMap addressMap, BlockingQueue<PutEvent> events) {
    addressMap_ = addressMap;
    putEvents_ = events;
  }

  @Override
  public InetSocketAddress getAddress(CommAddress commAddress) throws IOException {
    return addressMap_.getAddress(commAddress);
  }

  @Override
  public void putAddress(CommAddress commAddress, InetSocketAddress netAddress) throws IOException {
    addressMap_.putAddress(commAddress, netAddress);
    putEvents_.add(new PutEvent(commAddress, netAddress));

  }

  /**
   * @author Grzegorz Milka
   */
  public static final class PutEvent {
    private final CommAddress commAddress_;
    private final InetSocketAddress netAddress_;

    private PutEvent(CommAddress commAddress, InetSocketAddress netAddress) {
      commAddress_ = commAddress;
      netAddress_ = netAddress;
    }

    public CommAddress getCommAddress() {
      return commAddress_;
    }

    public InetSocketAddress getNetAddress() {
      return netAddress_;
    }
  }

}
