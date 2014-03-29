package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.log4j.Logger;

/**
 * @author Grzegorz Milka
 */
public class RemoteMapServerFactory implements RemoteMapFactory {
  private static final Logger LOGGER = Logger.getLogger(RemoteMapServerFactory.class);
  private final RemoteMap localMap_;
  private final ExecutorService executor_;
  private final ExecutorService workerExecutor_;
  private final int commPort_;

  private RemoteMapServer remoteMapServer_;
  private ServerSocket serverSocket_;

  @Inject
  public RemoteMapServerFactory(
      RemoteMap localMap,
      @Named("communication.remotemap.server-executor") ExecutorService executor,
      @Named("communication.remotemap.worker-executor") ExecutorService workerExecutor,
      @Named("communication.remotemap.local-port") int commPort) {
    localMap_ = localMap;
    executor_ = executor;
    workerExecutor_ = workerExecutor;
    commPort_ = commPort;
  }

  @Override
  public RemoteMap getRemoteMap() {
    return localMap_;
  }

  @Override
  public void startUp() throws IOException {
    LOGGER.debug("startUp()");
    serverSocket_ = new ServerSocket(commPort_);
    remoteMapServer_ = new RemoteMapServer(localMap_, serverSocket_, workerExecutor_);
    executor_.execute(remoteMapServer_);
  }

  @Override
  public void shutDown() {
    LOGGER.debug("shutDown()");
    try {
      serverSocket_.close();
    } catch (IOException e) {
      LOGGER.warn("IOException when closing server socket.", e);
    }
    LOGGER.debug("shutDown(): void");
  }
}
