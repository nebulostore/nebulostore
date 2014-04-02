package org.nebulostore.communication.routing;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grzegorz Milka
 */
public abstract class AbstractByteSenderTest {
  private static final int TEST_PORT = 3001;
  private static final byte[] TEST_MSG = {1, 2, 3, 4, 5};

  private ByteSender byteSender_;

  @Before
  public void setUp() {
    byteSender_ = newByteSender();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test(expected = IOException.class, timeout = 1000)
  public void shouldReturnErrorWhenSendingToNonexistentHost()
      throws InterruptedException, IOException {
    ByteSendFuture future = byteSender_.sendMessage(new InetSocketAddress(0), TEST_MSG);
    future.get();
  }

  @Test
  public void shouldSendCorrectlyFormattedMessageAndReturnResult()
      throws IOException, InterruptedException {
    ServerSocket socket = setUpListener(TEST_PORT);
    ByteSendFuture future = byteSender_.sendMessage(new InetSocketAddress(TEST_PORT), TEST_MSG);
    byte[] msg = receiveMessage(socket);
    assertArrayEquals(TEST_MSG, msg);
    assertTrue(future.get());
  }

  protected abstract ByteSender newByteSender();

  private byte[] receiveMessage(ServerSocket socket) throws IOException {
    try (Socket cliSocket = socket.accept()) {
      readVerField(cliSocket.getInputStream());
      int length = readLengthField(cliSocket.getInputStream());
      assertEquals(TEST_MSG.length, length);
      return readExactlyKBytes(cliSocket.getInputStream(), length);
    }
  }

  private byte[] readExactlyKBytes(InputStream is, int k) throws IOException {
    byte[] msg = new byte[k];
    int lastRead = 0;
    int len = 0;
    while (lastRead != -1 && len < k) {
      lastRead = is.read(msg, len, k - len);
      len += lastRead;
    }
    if (lastRead == -1) {
      throw new EOFException();
    }

    return msg;
  }

  private int readLengthField(InputStream is) throws IOException {
    byte[] lenField = readExactlyKBytes(is, ByteSender.INT_FIELD_LENGTH);
    ByteBuffer buf = ByteBuffer.wrap(lenField);
    return buf.getInt();
  }

  private void readVerField(InputStream is) throws IOException {
    byte[] verField = readExactlyKBytes(is, ByteSender.VERSION_FIELD_LENGTH);
    assertArrayEquals(ByteSender.VERSION_FIELD, verField);
  }

  private ServerSocket setUpListener(int testPort) throws IOException {
    ServerSocket socket = new ServerSocket(testPort);
    return socket;
  }
}
