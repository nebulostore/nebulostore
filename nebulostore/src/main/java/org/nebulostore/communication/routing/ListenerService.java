package org.nebulostore.communication.routing;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.messages.CommMessage;

/**
 * Service responsible for receiving CommMessages through TCP.
 *
 * Simply listens for incoming TCP connections and passes serialized
 *
 * @author Grzegorz Milka
 */
public class ListenerService implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(ListenerService.class);
  private final Executor serviceExecutor_;
  private final ExecutorService workerExecutor_;
  private final int commCliPort_;
  private final BlockingQueue<CommMessage> listeningQueue_;
  private ServerSocket serverSocket_;
  private Set<Socket> activeClientSockets_ = Collections.newSetFromMap(
      new ConcurrentHashMap<Socket, Boolean>());

  @Inject
  public ListenerService(@Named("communication.ports.comm-cli-port") int commCliPort,
      @Named("communication.routing.listening-queue") BlockingQueue<CommMessage> listeningQueue,
      @Named("communication.routing.listener-service-executor") Executor executor,
      @Named("communication.routing.listener-worker-executor") ExecutorService workExecutor) {
    commCliPort_ = commCliPort;
    serviceExecutor_ = executor;
    workerExecutor_ = workExecutor;
    listeningQueue_ = listeningQueue;
  }

  public BlockingQueue<CommMessage> getListeningQueue() {
    return listeningQueue_;
  }

  @Override
  public void run() {

    while (!serverSocket_.isClosed()) {
      Socket clientSocket = null;
      try {
        clientSocket = serverSocket_.accept();
      } catch (IOException e) {
        if (serverSocket_.isClosed()) {
          LOGGER.trace("IOException when accepting connection. Socket is closed.", e);
          break;
        } else {
          LOGGER.warn("IOException when accepting connection. Socket is open.", e);
          continue;
        }
      }
      LOGGER.trace("Accepted connection from: " + clientSocket.getRemoteSocketAddress());
      activeClientSockets_.add(clientSocket);
      workerExecutor_.execute(new ListenerProtocol(clientSocket));
    }

    shutDown();
    LOGGER.trace("run(): void");
  }

  public void start() throws IOException {
    startUp();
    serviceExecutor_.execute(this);
  }

  public void stop() {
    LOGGER.debug("stop()");
    try {
      serverSocket_.close();
    } catch (IOException e) {
      LOGGER.warn("IOException when closing server socket.", e);
    }
  }

  private void shutDown() {
    try {
      workerExecutor_.shutdownNow();
      shutDownClientSockets();
      workerExecutor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Unexpected interrupt.", e);
    }
  }

  private void shutDownClientSockets() {
    Iterator<Socket> iter = activeClientSockets_.iterator();
    while (iter.hasNext()) {
      try {
        iter.next().close();
      } catch (IOException e) {
        LOGGER.trace("shutDownClientSockets()", e);
      }
      iter.remove();
    }
  }

  private void startUp() throws IOException {
    serverSocket_ = new ServerSocket(commCliPort_);
    try {
      serverSocket_.setReuseAddress(true);
    } catch (SocketException e) {
      LOGGER.warn("Couldn't set serverSocket to reuse address: " + e);
    }
  }

  /**
   * Handler for incoming connection.
   *
   * @author Grzegorz Milka
   */
  private class ListenerProtocol implements Runnable {
    Socket clientSocket_;
    public ListenerProtocol(Socket clientSocket) {
      clientSocket_ = clientSocket;
    }

    public void run() {
      LOGGER.debug("ListenerProtocol.run() with " + clientSocket_.getRemoteSocketAddress());
      CommMessage msg = null;
      try {
        InputStream sis = clientSocket_.getInputStream();
        ObjectInputStream ois = new ObjectInputStream(sis);
        while (true) {
          msg = (CommMessage) ois.readObject();
          listeningQueue_.add(msg);
          LOGGER.trace("Added received message: " + msg + " to outgoing queue");
        }
      } catch (EOFException e) {
        LOGGER.trace("EOF in connection with: " + clientSocket_.getRemoteSocketAddress(), e);
      } catch (ClassNotFoundException | IOException e) {
        if (!serverSocket_.isClosed()) {
          LOGGER.warn("Error when handling message from " + clientSocket_.getRemoteSocketAddress(),
              e);
        }
      } finally {
        LOGGER.trace("Closing ListenerProtocol connection with host: " +
          clientSocket_.getRemoteSocketAddress());
        try {
          clientSocket_.close();
        } catch (IOException e) {
          LOGGER.trace("IOException when closing client's socket to: " +
            clientSocket_.getRemoteSocketAddress(), e);
        }
        activeClientSockets_.remove(clientSocket_);
      }
    }
  }
}
