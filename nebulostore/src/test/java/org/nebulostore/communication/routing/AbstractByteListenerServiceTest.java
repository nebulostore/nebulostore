package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 *
 * @author Grzegorz Milka
 */
public abstract class AbstractByteListenerServiceTest {
  protected static final int LISTENER_SERVICE_TEST_PORT = 3000;
  private static final byte[] TEST_MSG = {0, 1, 2, 3, 4, 5};
  private static final byte[] VER_FIELD = {0, 1};

  private ByteListenerService listenerService_;

  @Before
  public void setUp() throws IOException {
    listenerService_ = newByteListenerService();
    listenerService_.startUp();
  }

  @After
  public void tearDown() {
    listenerService_.shutDown();
  }

  @Test
  public void shouldGetAndForwardMessage() throws Exception {
    sendTestMessageAndCheckReceival();
  }

  @Test
  public void shouldBeOperationalAfterWrongVersionFieldFailure() throws Exception {
    sendIncorrectlyVersionedMessage();
    sendTestMessageAndCheckReceival();
  }

  @Test
  public void shouldBeOperationalAfterTooShortMessageFailure() throws Exception {
    sendTooShortMessage();
    sendTestMessageAndCheckReceival();
  }

  @Test(timeout = 1000)
  public void shouldNotifyObserversWhenNewMessageArrives() throws Exception {
    BlockingObserver observer = new BlockingObserver();
    listenerService_.addObserver(observer);
    sendTestMessageAndCheckReceival();
    observer.awaitUpdate();
  }

  protected abstract ByteListenerService newByteListenerService();

  private static class BlockingObserver implements Observer {
    private AtomicBoolean hasUpdated_ = new AtomicBoolean(false);

    public synchronized void awaitUpdate() throws InterruptedException {
      while (!hasUpdated_.get()) {
        this.wait();
      }
    }

    @Override
    public synchronized void update(Observable o, Object arg) {
      hasUpdated_.set(true);
      this.notifyAll();
    }

  }

  private Socket createSocketToListener() throws IOException {
    return new Socket(InetAddress.getByName("localhost"), LISTENER_SERVICE_TEST_PORT);
  }

  private void sendIncorrectlyVersionedMessage() throws IOException {
    try (Socket socket = createSocketToListener()) {
      byte[] verField = {0, 2};
      socket.getOutputStream().write(verField);
      socket.getOutputStream().write(getLengthField(TEST_MSG.length));
      socket.getOutputStream().write(TEST_MSG);
    }

  }

  private void sendMessageThroughSocket(Socket socket, byte[] testMsg) throws IOException {
    byte[] lenField = getLengthField(testMsg.length);
    socket.getOutputStream().write(VER_FIELD);
    socket.getOutputStream().write(lenField);
    socket.getOutputStream().write(testMsg);
  }

  private void sendTestMessageAndCheckReceival() throws InterruptedException, IOException {
    try (Socket socket = createSocketToListener()) {
      sendMessageThroughSocket(socket, TEST_MSG);
    }
    byte[] recv = listenerService_.getListeningQueue().take();
    assertArrayEquals(TEST_MSG, recv);
  }

  private void sendTooShortMessage() throws IOException {
    try (Socket socket = createSocketToListener()) {
      sendTooShortMessageThroughSocket(socket, TEST_MSG);
    }

  }

  private void sendTooShortMessageThroughSocket(Socket socket, byte[] testMsg) throws IOException {
    byte[] lenField = getLengthField(testMsg.length + 1);
    socket.getOutputStream().write(VER_FIELD);
    socket.getOutputStream().write(lenField);
    socket.getOutputStream().write(testMsg);
  }

  private byte[] getLengthField(int length) {
    byte[] lenField = new byte[ByteSender.INT_FIELD_LENGTH];
    ByteBuffer buf = ByteBuffer.wrap(lenField);
    buf.order(ByteOrder.BIG_ENDIAN);
    buf.putInt(length);
    return buf.array();
  }

}
