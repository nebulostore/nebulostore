package org.nebulostore.communication.routing;

import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.messages.CommMessage;

import static org.mockito.Mockito.mock;

/**
 *
 * @author Grzegorz Milka
 */
public final class ListenerServiceTest {
  private static final int LISTENER_SERVICE_TEST_PORT = 3000;

  private ExecutorService serviceExecutor_;
  private ExecutorService workerExecutor_;
  private ListenerService listenerService_;

  @Before
  public void setUp() {
    serviceExecutor_ = Executors.newFixedThreadPool(1);
    workerExecutor_ = Executors.newCachedThreadPool();

    AbstractModule initModule = new ListenerServiceTestModule(serviceExecutor_, workerExecutor_);
    Injector injector = Guice.createInjector(initModule);
    listenerService_ = injector.getInstance(ListenerService.class);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void shouldGetAndForwardMessage() throws Exception {
    listenerService_.start();
    try (Socket socket = createSocketToListener()) {
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      CommMessage commMessage = mock(CommMessage.class);
      oos.writeObject(commMessage);
      oos.close();
    }
    listenerService_.getListeningQueue().take();
    listenerService_.stop();
  }

  private Socket createSocketToListener() throws Exception {
    return new Socket(InetAddress.getByName("localhost"), LISTENER_SERVICE_TEST_PORT);
  }

  /**
   *
   * @author Grzegorz Milka
   */
  private static class ListenerServiceTestModule extends AbstractModule {
    private final Executor serviceExecutor_;
    private final ExecutorService workerExecutor_;

    public ListenerServiceTestModule(Executor serviceExecutor,
        ExecutorService workerExecutor) {
      serviceExecutor_ = serviceExecutor;
      workerExecutor_ = workerExecutor;
    }

    @Override
    protected void configure() {
      bindConstant().annotatedWith(Names.named("communication.ports.comm-cli-port")).
        to(LISTENER_SERVICE_TEST_PORT);

      bind(new TypeLiteral<BlockingQueue<CommMessage>>() { }).
        annotatedWith(Names.named("communication.routing.listening-queue")).
        toInstance(new LinkedBlockingQueue<CommMessage>());

      bind(Executor.class).
        annotatedWith(Names.named("communication.routing.listener-service-executor")).
        toInstance(serviceExecutor_);

      bind(ExecutorService.class).
        annotatedWith(Names.named("communication.routing.listener-worker-executor")).
        toInstance(workerExecutor_);

      bind(ListenerService.class);
    }
  }

}
