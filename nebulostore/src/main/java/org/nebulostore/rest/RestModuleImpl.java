package org.nebulostore.rest;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spi.Container;
import org.glassfish.jersey.server.spi.ContainerLifecycleListener;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
public class RestModuleImpl implements RestModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestModuleImpl.class);
  private final AtomicBoolean isTerminate_;
  private final String host_;
  private final int port_;
  private final Injector injector_;

  @Inject
  public RestModuleImpl(@Named("rest-api.server-config.host") String host,
                        @Named("rest-api.server-config.port") int port,
                        Injector injector) {
    host_ = host;
    port_ = port;
    injector_ = injector;
    isTerminate_ = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    URI uri = URI.create(String.format("%s:%s/", host_, port_));
    ResourceConfig config = configureServer();
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, config);

    try {
      httpServer.start();
      waitForTermination();
      httpServer.shutdown();
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage());
    }

  }

  private void waitForTermination() throws InterruptedException {
    synchronized (isTerminate_) {
      while (!isTerminate_.get()) {
        isTerminate_.wait();
      }
    }
  }

  @Override
  public void shutDown() {
    synchronized (isTerminate_) {
      isTerminate_.set(true);
      isTerminate_.notify();
    }
  }

  private ResourceConfig configureServer() {
    ResourceConfig resourceConfig = new RestResourceConfig(MultiPartFeature.class);
    resourceConfig.packages(true, "org.nebulostore");
    return resourceConfig;
  }

  private class RestResourceConfig extends ResourceConfig {

    public RestResourceConfig(Class<?>... classes) {
      super(classes);

      register(new ContainerLifecycleListener() {

        @Override
        public void onStartup(Container container) {
          ServiceLocator serviceLocator = container.getApplicationHandler().getServiceLocator();

          GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);
          GuiceIntoHK2Bridge guiceBridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);
          guiceBridge.bridgeGuiceInjector(injector_);
        }

        @Override
        public void onShutdown(Container container) {
        }

        @Override
        public void onReload(Container container) {
        }
      });
    }
  }

}
