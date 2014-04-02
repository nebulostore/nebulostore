package org.nebulostore.communication.routing.plainsocket;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.Iterator;
import java.util.Observable;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.routing.ByteListenerService;
import org.nebulostore.communication.routing.ByteSender;

/**
 * Implementation of {@link ByteListenerService} using Java's sockets.
 *
 * @author Grzegorz Milka
 */
public class PlainSocketByteListenerService extends Observable
    implements ByteListenerService, Runnable {
  private static final Logger LOGGER = Logger.getLogger(PlainSocketByteListenerService.class);
  private final Executor serviceExecutor_;
  private final ExecutorService workerExecutor_;
  private final int commCliPort_;
  private final BlockingQueue<byte[]> listeningQueue_;
  private ServerSocket serverSocket_;
  private Set<Socket> activeClientSockets_ = Collections.newSetFromMap(
      new ConcurrentHashMap<Socket, Boolean>());

  @Inject
  public PlainSocketByteListenerService(@Named("communication.ports.comm-cli-port") int commCliPort,
      @Named("communication.routing.byte-listening-queue") BlockingQueue<byte[]> listeningQueue,
      @Named("communication.routing.listener-service-executor") Executor executor,
      @Named("communication.routing.listener-worker-executor") ExecutorService workExecutor) {
    commCliPort_ = commCliPort;
    serviceExecutor_ = executor;
    workerExecutor_ = workExecutor;
    listeningQueue_ = listeningQueue;
  }

  public BlockingQueue<byte[]> getListeningQueue() {
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
          LOGGER.info("IOException when accepting connection. Socket is open.", e);
          continue;
        }
      }
      LOGGER.trace("Accepted connection from: " + clientSocket.getRemoteSocketAddress());
      activeClientSockets_.add(clientSocket);
      workerExecutor_.execute(new ListenerProtocol(clientSocket));
    }

    stop();
    LOGGER.trace("run(): void");
  }

  public void startUp() throws IOException {
    LOGGER.debug("startUp()");
    start();
    serviceExecutor_.execute(this);
  }

  public void shutDown() {
    LOGGER.debug("shutDown()");
    try {
      serverSocket_.close();
    } catch (IOException e) {
      LOGGER.warn("IOException when closing server socket.", e);
    }
  }

  private void start() throws IOException {
    serverSocket_ = new ServerSocket(commCliPort_);
    try {
      serverSocket_.setReuseAddress(true);
    } catch (SocketException e) {
      LOGGER.warn("Couldn't set serverSocket to reuse address: " + e);
    }
  }

  private void stop() {
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
      byte[] msg;
      try {
        InputStream is = clientSocket_.getInputStream();
        while (true) {
          int version = getVersion(is);
          if (version == -1) {
            break;
          } else if (version != ByteSender.VERSION) {
            LOGGER.warn(String.format("Unexpected version number received: %d.", version));
            break;
          }
          int length = getLength(is);
          msg = readExactlyKBytes(is, length);
          listeningQueue_.add(msg);
          setChanged();
          notifyObservers();
          LOGGER.debug("Added received message: " + msg + " to outgoing queue");
        }
      } catch (EOFException e) {
        LOGGER.trace("EOF in connection with: " + clientSocket_.getRemoteSocketAddress(), e);
      } catch (IOException e) {
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

    private ByteBuffer getField(InputStream is, int length) throws IOException {
      byte[] ver = readExactlyKBytes(is, length);
      ByteBuffer buf = ByteBuffer.wrap(ver);
      buf.order(ByteOrder.BIG_ENDIAN);
      return buf;
    }

    private int getLength(InputStream is) throws IOException {
      return getField(is, ByteSender.INT_FIELD_LENGTH).getInt();
    }

    private int getVersion(InputStream is) throws IOException {
      try {
        return getField(is, ByteSender.VERSION_FIELD_LENGTH).getShort();
      } catch (EOFException e) {
        return -1;
      }
    }

    private byte[] readExactlyKBytes(InputStream is, int k) throws IOException {
      byte[] arr = new byte[k];
      int read = 0;
      int lastRead = 0;
      while (lastRead != -1 && read < k) {
        lastRead = is.read(arr, read, k - read);
        read += lastRead;
      }
      if (lastRead == -1) {
        throw new EOFException(String.format("Couldn't read %d bytes from stream.", k));
      }
      return arr;
    }
  }
}
