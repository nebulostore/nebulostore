package org.nebulostore.communication.routing.plainsocket;

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

import org.nebulostore.communication.routing.AbstractByteListenerServiceTest;
import org.nebulostore.communication.routing.ByteListenerService;

public final class PlainSocketByteListenerServiceTest extends AbstractByteListenerServiceTest {
  @Override
  protected ByteListenerService newByteListenerService() {
    ExecutorService serviceExecutor = Executors.newFixedThreadPool(1);
    ExecutorService workerExecutor = Executors.newFixedThreadPool(1);

    AbstractModule initModule = new ListenerServiceTestModule(serviceExecutor, workerExecutor);
    Injector injector = Guice.createInjector(initModule);
    return injector.getInstance(ByteListenerService.class);
  }

  /**
   *
   * @author Grzegorz Milka
   */
  private static class ListenerServiceTestModule extends AbstractModule {
    private final Executor serviceExecutor_;
    private final ExecutorService workerExecutor_;

    public ListenerServiceTestModule(Executor serviceExecutor, ExecutorService workerExecutor) {
      serviceExecutor_ = serviceExecutor;
      workerExecutor_ = workerExecutor;
    }

    @Override
    protected void configure() {
      bindConstant().annotatedWith(Names.named("communication.ports.comm-cli-port")).to(
          LISTENER_SERVICE_TEST_PORT);

      bind(new TypeLiteral<BlockingQueue<byte[]>>() {
      }).annotatedWith(Names.named("communication.routing.byte-listening-queue")).toInstance(
          new LinkedBlockingQueue<byte[]>());

      bind(Executor.class).annotatedWith(
          Names.named("communication.routing.listener-service-executor")).toInstance(
          serviceExecutor_);

      bind(ExecutorService.class).annotatedWith(
          Names.named("communication.routing.listener-worker-executor"))
          .toInstance(workerExecutor_);

      bind(ByteListenerService.class).to(PlainSocketByteListenerService.class);
    }
  }
}
