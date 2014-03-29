package org.nebulostore.communication.bootstrap;

import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertEquals;

/**
 * @author Grzegorz Milka
 */
public class BootstrapServerTest {
  private static final int BOOTSTRAP_SERVER_TEST_PORT = 7585;
  private static final CommAddress LOCAL_COMM_ADDRESS = new CommAddress(0, 0);

  private ExecutorService mainExecutor_;
  private ExecutorService workerExecutor_;
  private BootstrapServer bootstrapServer_;

  @Before
  public void setUp() {
    mainExecutor_ = Executors.newFixedThreadPool(1);
    workerExecutor_ = Executors.newCachedThreadPool();

    AbstractModule initModule = new BootstrapServerTestModule(mainExecutor_, workerExecutor_);
    Injector injector = Guice.createInjector(initModule);
    bootstrapServer_ = injector.getInstance(BootstrapServer.class);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void clientShouldGetCorrectInformation() throws Exception {
    BootstrapInformation bootInfo;
    bootstrapServer_.startUp();
    try (Socket socket = createSocketToServer()) {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      bootInfo = (BootstrapInformation) ois.readObject();
      ois.close();
    }
    bootstrapServer_.shutDown();

    Collection<CommAddress> returnedCommAddresses = bootInfo.getBootstrapCommAddresses();
    assertEquals(1, returnedCommAddresses.size());
    assertEquals(LOCAL_COMM_ADDRESS, returnedCommAddresses.iterator().next());
  }

  private Socket createSocketToServer() throws Exception {
    return new Socket(InetAddress.getLocalHost(), BOOTSTRAP_SERVER_TEST_PORT);
  }

  /**
   *
   * @author Grzegorz Milka
   */
  private static class BootstrapServerTestModule extends AbstractModule {
    private final ExecutorService serviceExecutor_;
    private final ExecutorService workerExecutor_;

    public BootstrapServerTestModule(ExecutorService serviceExecutor,
        ExecutorService workerExecutor) {
      serviceExecutor_ = serviceExecutor;
      workerExecutor_ = workerExecutor;
    }

    @Override
    protected void configure() {
      bindConstant().annotatedWith(Names.named("communication.ports.bootstrap-server-port")).
        to(BOOTSTRAP_SERVER_TEST_PORT);

      bind(CommAddress.class).
        annotatedWith(Names.named("communication.local-comm-address")).
        toInstance(LOCAL_COMM_ADDRESS);

      bind(ExecutorService.class).
        annotatedWith(Names.named("communication.bootstrap.server-executor")).
        toInstance(serviceExecutor_);

      bind(ExecutorService.class).
        annotatedWith(Names.named("communication.bootstrap.worker-executor")).
        toInstance(workerExecutor_);

      bind(BootstrapServer.class);
    }
  }
}
