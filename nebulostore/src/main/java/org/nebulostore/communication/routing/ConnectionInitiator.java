package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Initiator of socket connections to specified address.
 *
 * Creates a valid socket connection to specified address.
 *
 * @author Grzegorz Milka
 */
public interface ConnectionInitiator {
  Socket newConnection(InetSocketAddress address) throws IOException;
}
