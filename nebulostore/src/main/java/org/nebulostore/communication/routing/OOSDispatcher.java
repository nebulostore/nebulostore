package org.nebulostore.communication.routing;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;

/**
 * <p>
 * Creates, caches and distributes object output streams connected to other nodes.
 * </p>
 *
 * <p>
 * Object of this class optimizes connections by giving only one stream to other host at the time
 * and by caching them for a short time for later reuse.
 * </p>
 *
 * <p>
 * Every object needs to be started before use and stopped after.
 * </p>
 *
 * @author Grzegorz Milka
 */
public interface OOSDispatcher {
  /**
   * <p>
   * Creates or gets cached {@link ObjectOutputStream} to given address. May block if
   * other thread uses a connection to that address.
   * </p>
   *
   * <p>
   * Every stream got by this function needs to be returned using putStream.
   * <p>
   *
   * @param address
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  ObjectOutputStream getStream(InetSocketAddress address) throws InterruptedException, IOException;

  void putStream(InetSocketAddress address);

  /**
   * Starts threads responsible for caching and closing streams.
   */
  void startUp();

  void shutDown();
}
