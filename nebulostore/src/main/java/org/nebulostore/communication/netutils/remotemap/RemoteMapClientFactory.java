package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;

/**
 * Factory for client remote map.
 *
 * @author Grzegorz Milka
 *
 */
public class RemoteMapClientFactory implements RemoteMapFactory {
  private static final Logger LOGGER = Logger.getLogger(RemoteMapClientFactory.class);
  private RemoteMap remoteMap_;

  @Inject
  public RemoteMapClientFactory(
      @Named("communication.remotemap.server-net-address")
      InetSocketAddress serverAddress) {
    remoteMap_ = new RemoteMapClient(serverAddress);
  }

  @Override
  public RemoteMap getRemoteMap() {
    return remoteMap_;
  }

  @Override
  public void startUp() throws IOException {
    LOGGER.debug("startUp()");
  }

  @Override
  public void shutDown() {
    LOGGER.debug("shutDown()");
  }
}
