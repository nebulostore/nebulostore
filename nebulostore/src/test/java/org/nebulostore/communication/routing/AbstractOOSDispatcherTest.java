package org.nebulostore.communication.routing;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Grzegorz Milka
 */
public abstract class AbstractOOSDispatcherTest {
  private static final int LISTENING_PORT = 3001;
  protected OOSDispatcher dispatcher_;

  @Before
  public void setUp() {
    dispatcher_.startUp();
  }

  @After
  public void tearDown() {
    dispatcher_.shutDown();
  }

  @Test
  public void shouldGetCorrectOOS() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(LISTENING_PORT)) {
      InetSocketAddress localAddress = new InetSocketAddress(LISTENING_PORT);
      ObjectOutputStream oos = dispatcher_.getStream(localAddress);
      oos.writeObject(new Integer(LISTENING_PORT));
      assertEquals(new Integer(LISTENING_PORT), readFromServerSocket(serverSocket));
      dispatcher_.putStream(localAddress);
    }
  }

  @Test(expected = IOException.class)
  public void shouldReturnExceptionWhenCannotInitiateConnection() throws Exception {
    InetSocketAddress localAddress = new InetSocketAddress(LISTENING_PORT);
    dispatcher_.getStream(localAddress);
  }

  private Object readFromServerSocket(ServerSocket serverSocket) throws Exception {
    Socket socket = serverSocket.accept();
    try {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      return ois.readObject();
    } finally {
      socket.close();
    }
  }
}
