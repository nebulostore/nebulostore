package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Straightforward initiator creating TCP sockets to given address.
 *
 * @author Grzegorz Milka
 */
public class SimpleTCPConnectionInitator implements ConnectionInitiator {
  @Override
  public Socket newConnection(InetSocketAddress address) throws IOException {
    return new Socket(address.getAddress(), address.getPort());
  }
}
