package org.nebulostore.communication.naming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;

/**
 * Decorator adding caching of addresses.
 * @author Grzegorz Milka
 */
public class CachedCommAddressResolver implements CommAddressResolver {
  private static final Logger LOGGER = Logger.getLogger(CachedCommAddressResolver.class);

  private static final long CACHE_TIMEOUT = 60;
  private final ScheduledExecutorService executor_;
  private final Map<CommAddress, InetSocketAddress> cache_ =
    Collections.synchronizedMap(new HashMap<CommAddress, InetSocketAddress>());
  private final Map<CommAddress, AtomicBoolean> usageBits_ =
    Collections.synchronizedMap(new HashMap<CommAddress, AtomicBoolean>());
  private final AtomicBoolean hasStarted_ = new AtomicBoolean(false);

  private CommAddressResolver resolver_;


  @Inject
  public CachedCommAddressResolver(
        @Named("communication.naming.cached-base-resolver") CommAddressResolver resolver,
        @Named("communication.naming.cached-scheduled-executor")
          ScheduledExecutorService executor) {
    resolver_ = resolver;
    executor_ = executor;
  }

  @Override
  public InetSocketAddress resolve(CommAddress commAddress)
      throws IOException, AddressNotPresentException {
    LOGGER.trace(String.format("resolve(%s)", commAddress));

    if (hasStarted_.compareAndSet(false, true)) {
      executor_.scheduleWithFixedDelay(new CacheCleaner(), CACHE_TIMEOUT, CACHE_TIMEOUT,
          TimeUnit.SECONDS);
    }
    InetSocketAddress inetSocketAddress;

    AtomicBoolean flag = usageBits_.get(commAddress);
    if (flag == null) {
      flag = new AtomicBoolean(true);
      synchronized (flag) {
        LOGGER.trace(String.format("resolve(%s) -> No address in cache.", commAddress));
        inetSocketAddress = resolver_.resolve(commAddress);
        usageBits_.put(commAddress, flag);
        cache_.put(commAddress, inetSocketAddress);
      }
    } else {
      synchronized (flag) {
        inetSocketAddress = cache_.get(commAddress);
        if (inetSocketAddress == null) {
          LOGGER.trace(String.format("resolve(%s) -> No address in cache.", commAddress));
          inetSocketAddress = resolver_.resolve(commAddress);
        }
        flag.set(true);
        usageBits_.put(commAddress, flag);
        cache_.put(commAddress, inetSocketAddress);
      }
    }

    LOGGER.trace(String.format("resolve(%s): %s", commAddress, inetSocketAddress));
    return inetSocketAddress;
  }

  @Override
  public void reportFailure(CommAddress commAddress) {
    LOGGER.debug(String.format("reportFailure(%s)", commAddress));
    clearCache(commAddress);
  }

  @Override
  public void shutDown() {
    executor_.shutdown();
    try {
      executor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new IllegalStateException();
    }
    LOGGER.trace("shutDown(): void");
  }

  @Override
  public String toString() {
    return String.format("CachedAddressResolver[resolver: %s]",  resolver_);
  }

  private void clearCache(CommAddress commAddress) {
    AtomicBoolean flag = usageBits_.get(commAddress);
    if (flag != null) {
      synchronized (flag) {
        usageBits_.remove(commAddress);
        cache_.remove(commAddress);
      }
    }
  }

  /**
   * Cleans cache periodically.
   */
  private class CacheCleaner implements Runnable {

    @Override
    public void run() {
      LOGGER.trace("CacheCleaner.run()");
      Set<Entry<CommAddress, AtomicBoolean>> entrySet = usageBits_.entrySet();
      Iterator<Entry<CommAddress, AtomicBoolean>> iter = entrySet.iterator();

      while (iter.hasNext()) {
        Entry<CommAddress, AtomicBoolean> entry = iter.next();
        synchronized (entry.getValue()) {
          if (entry.getValue().get()) {
            entry.getValue().set(false);
          } else {
            cache_.remove(entry.getKey());
            iter.remove();
          }
        }
      }
      LOGGER.trace("CacheCleaner.run(): void");
    }
  }
}
