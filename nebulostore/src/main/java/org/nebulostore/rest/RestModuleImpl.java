package org.nebulostore.rest;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lukaszsiczek
 */
public class RestModuleImpl implements RestModule {

  private static final Logger LOGGER = LoggerFactory.getLogger(RestModuleImpl.class);
  private AtomicBoolean isTerminate_;
  private String host_;
  private int port_;
  private BrokerResource brokerResource_;
  private NetworkMonitorResource networkMonitorResource_;
  private ReplicatorResource replicatorResource_;

  @Inject
  public RestModuleImpl(@Named("rest-api.server-config.host") String host,
                        @Named("rest-api.server-config.port") int port,
                        BrokerResource brokerResource,
                        NetworkMonitorResource networkMonitorResource,
                        ReplicatorResource replicatorResource) {
    host_ = host;
    port_ = port;
    isTerminate_ = new AtomicBoolean(false);
    brokerResource_ = brokerResource;
    networkMonitorResource_ = networkMonitorResource;
    replicatorResource_ = replicatorResource;
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

  private ResourceConfig configureServer() {
    ResourceConfig resourceConfig = new ResourceConfig(MultiPartFeature.class);
    resourceConfig.register(
        new ServerTerminationResource(isTerminate_));
    resourceConfig.register(brokerResource_);
    resourceConfig.register(networkMonitorResource_);
    resourceConfig.register(replicatorResource_);
    return resourceConfig;
  }

}
