package org.nebulostore.communication.naming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.addressmap.AddressMap;
import org.nebulostore.communication.naming.addressmap.AddressMapFactory;
import org.nebulostore.communication.netutils.NetworkAddressDiscovery;

/**
 * This object is responsible for keeping the mapping from {@link CommAddress} to
 * Net address up to date in address map.
 *
 * This object is service that should be started and stopped to work.
 *
 * @author Grzegorz Milka
 *
 */
public class AddressMappingMaintainer {
  private static final Logger LOGGER = Logger.getLogger(AddressMappingMaintainer.class);
  private final AddressMapFactory addressMapFactory_;
  private AddressMap addressMap_;
  private final NetworkAddressDiscovery networkAddressDiscovery_;
  private final CommAddress localCommAddress_;
  private final ScheduledExecutorService scheduledExecutor_;

  private final Observer addressChangeObserver_;

  private final AddressMappingChecker amChecker_;
  private final long checkInterval_;
  private final TimeUnit checkIntervalTimeUnit_;

  private InetSocketAddress networkAddress_;
  private ScheduledFuture<?> scheduledChecker_;

  @Inject
  public AddressMappingMaintainer(
      AddressMapFactory addressMapFactory,
      NetworkAddressDiscovery networkAddressDiscovery,
      @Named("communication.local-comm-address") CommAddress localCommAddress,
      @Named("communication.address-map-maintainer-scheduled-executor")
        ScheduledExecutorService scheduledExecutor,
      @Named("communication.address-map-check-interval") int checkInterval,
      @Named("communication.address-map-check-interval-unit") TimeUnit checkIntervalUnit) {
    addressMapFactory_ = addressMapFactory;
    networkAddressDiscovery_ = networkAddressDiscovery;
    localCommAddress_ = localCommAddress;
    scheduledExecutor_ = scheduledExecutor;

    addressChangeObserver_ = new AddressChangeObserver();

    amChecker_ = new AddressMappingChecker();
    checkInterval_ = checkInterval;
    checkIntervalTimeUnit_ = checkIntervalUnit;
  }

  public void startUp() {
    addressMap_ = addressMapFactory_.getAddressMap();
    networkAddress_ = networkAddressDiscovery_.getNetworkAddress();
    networkAddressDiscovery_.addObserver(addressChangeObserver_);
    amChecker_.run();
    scheduledChecker_ = scheduledExecutor_.scheduleWithFixedDelay(amChecker_, 0, checkInterval_,
        checkIntervalTimeUnit_);
  }

  public void shutDown() {
    scheduledExecutor_.shutdown();
    scheduledChecker_.cancel(true);
    try {
      scheduledExecutor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
    networkAddressDiscovery_.deleteObserver(addressChangeObserver_);
    LOGGER.trace("shutDown(): void");
  }

  /**
   * @author Grzegorz Milka
   */
  private class AddressChangeObserver implements Observer {
    @Override
    public void update(Observable addressDiscovery, Object newAddress) {
      InetSocketAddress newInetAddress = (InetSocketAddress) newAddress;
      if (!newInetAddress.equals(networkAddress_)) {
        networkAddress_ = newInetAddress;
        scheduledExecutor_.execute(amChecker_);
      }
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private class AddressMappingChecker implements Runnable {
    public void run() {
      try {
        InetSocketAddress netAddress = addressMap_.getAddress(localCommAddress_);
        if (networkAddress_ != null && !networkAddress_.equals(netAddress)) {
          addressMap_.putAddress(localCommAddress_, networkAddress_);
        }
      } catch (IOException e) {
        LOGGER.warn("Couldn't get or put current network address for local comm address.", e);
      }
    }
  }
}
