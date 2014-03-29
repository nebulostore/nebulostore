package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;

/**
 * Factory for producing RemoteMap clients and also maintaining some properties of remote map.
 *
 * @author Grzegorz Milka
 */
public interface RemoteMapFactory {
  RemoteMap getRemoteMap();
  void startUp() throws IOException;
  void shutDown();
}
