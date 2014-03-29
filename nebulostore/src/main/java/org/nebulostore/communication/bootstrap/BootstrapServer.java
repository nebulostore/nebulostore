package org.nebulostore.communication.bootstrap;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Bootstrap server.
 *
 * @author Grzegorz Milka
 *
 */
public class BootstrapServer implements BootstrapService, Runnable {
  private static final Logger LOGGER = Logger.getLogger(BootstrapService.class);

  private final int listeningPort_;

  private final ExecutorService mainExecutor_;
  private final ExecutorService workerExecutor_;

  private Future<?> thisTask_;
  private ServerSocket serverSocket_;

  private final AtomicBoolean isShutdown_;

  private final BootstrapInformation bootstrapInformation_;

  @Inject
  public BootstrapServer(
      @Named("communication.local-comm-address") CommAddress localCommAddress,
      @Named("communication.ports.bootstrap-server-port") int listeningPort,
      @Named("communication.bootstrap.server-executor") ExecutorService mainExecutor,
      @Named("communication.bootstrap.worker-executor") ExecutorService workerExecutor) {
    listeningPort_ = listeningPort;

    mainExecutor_ = mainExecutor;
    workerExecutor_ = workerExecutor;

    isShutdown_ = new AtomicBoolean(false);

    Collection<CommAddress> commAddressCollection = new LinkedList<>();
    commAddressCollection.add(localCommAddress);
    bootstrapInformation_ = new BootstrapInformation(commAddressCollection);
  }

  @Override
  public BootstrapInformation getBootstrapInformation() {
    return bootstrapInformation_;
  }

  public void run() {
    while (!isShutdown_.get()) {
      Socket incomingConnection;
      try {
        incomingConnection = serverSocket_.accept();
      } catch (IOException e) {
        if (isShutdown_.get()) {
          break;
        } else {
          LOGGER.warn("BootstrapServer socket threw exception on accept.", e);
          continue;
        }
      }
      Runnable handlerTask = new BootstrapInformationSender(incomingConnection);
      workerExecutor_.submit(handlerTask);
    }
  }

  @Override
  public synchronized void shutDown() throws InterruptedException {
    isShutdown_.set(true);

    workerExecutor_.shutdownNow();
    workerExecutor_.awaitTermination(1, TimeUnit.DAYS);

    try {
      serverSocket_.close();
    } catch (IOException e) {
      LOGGER.warn("IOException when closing server socket.", e);
    }

    try {
      thisTask_.get();
    } catch (ExecutionException e) {
      throw new IllegalStateException("Unexpected exception", e);
    }
  }

  @Override
  public synchronized void startUp() throws IOException {
    serverSocket_ = new ServerSocket(listeningPort_);
    try {
      serverSocket_.setReuseAddress(true);
    } catch (SocketException e) {
      LOGGER.warn("Couldn't set serverSocket of bootstrap to reuse address.", e);
    }

    thisTask_ = mainExecutor_.submit(this);
  }

  /**
   * Sender of bootstrap information to connection client.
   *
   * @author Grzegorz Milka
   *
   */
  private class BootstrapInformationSender implements Runnable {
    private final Socket clientSocket_;

    public BootstrapInformationSender(Socket clientSocket) {
      clientSocket_ = clientSocket;
    }

    @Override
    public void run() {
      try {
        ObjectOutputStream oos = new ObjectOutputStream(clientSocket_.getOutputStream());
        oos.writeObject(bootstrapInformation_);
        oos.flush();
      } catch (IOException e) {
        LOGGER.warn("Exception when sending bootstrap information to client.", e);
      } finally {
        try {
          clientSocket_.close();
        } catch (IOException e) {
          /* IGNORE */
          this.hashCode();
        }
      }
    }
  }

}
