package org.nebulostore.communication.routing;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for ConnectionInitiators.
 *
 * @author Grzegorz Milka
 */
public abstract class AbstractConnectionInitatorTest {
  protected ConnectionInitiator initiator_;
  private static final int CONN_INIT_TEST_PORT = 3001;
  private static InetSocketAddress localhostSocketAddr_;

  @BeforeClass
  public static void setUpClass() throws Exception {
    localhostSocketAddr_ = new InetSocketAddress("localhost", CONN_INIT_TEST_PORT);
  }

  @Test
  public void shouldCreateValidSocket() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(CONN_INIT_TEST_PORT)) {
      try (Socket socket = initiator_.newConnection(localhostSocketAddr_)) {
        socket.getOutputStream().write(0);
      }
      try (Socket socket = serverSocket.accept()) {
        assertEquals(0, socket.getInputStream().read());
      }
    }
  }
}
