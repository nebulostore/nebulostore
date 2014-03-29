package org.nebulostore.communication.bootstrap;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertEquals;

/**
 * @author Grzegorz Milka
 */
public class BootstrapClientTest {
  private static final CommAddress SERVER_COMM_ADDRESS = new CommAddress(0, 0);
  private static final int BOOTSTRAP_CLIENT_TEST_PORT = 7977;
  private BootstrapService client_;

  private BootstrapServer server_;

  @Before
  public void setUp() throws Exception {
    InetSocketAddress serverAddress = new InetSocketAddress(BOOTSTRAP_CLIENT_TEST_PORT);
    Collection<InetSocketAddress> socketAddresses = new LinkedList<>();
    socketAddresses.add(serverAddress);

    client_ = new BootstrapClient(socketAddresses);
  }

  @Test
  public void shouldGetCorrectInformation() throws Exception {
    ExecutorService mainExecutor = Executors.newFixedThreadPool(1);
    ExecutorService workerExecutor = Executors.newFixedThreadPool(1);
    server_ = new BootstrapServer(SERVER_COMM_ADDRESS, BOOTSTRAP_CLIENT_TEST_PORT,
        mainExecutor, workerExecutor);
    server_.startUp();
    try {
      client_.startUp();
      BootstrapInformation bootInfo = client_.getBootstrapInformation();
      assertEquals(1, bootInfo.getBootstrapCommAddresses().size());
      assertEquals(SERVER_COMM_ADDRESS, bootInfo.getBootstrapCommAddresses().iterator().next());
    } finally {
      server_.shutDown();
    }
  }

  @Test(expected = IOException.class)
  public void shouldThrowExceptionDuringStartUpWhenServerIsNotWorking() throws Exception {
    client_.startUp();
  }


}
