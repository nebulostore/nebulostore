package org.nebulostore.newcommunication.netutils.remotemap;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import org.apache.log4j.Logger;

/**
 * RMI Server Factory for RemoteMap.
 *
 * @author Grzegorz Milka
 */
public class RemoteMapServer implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(RemoteMapServer.class);
  private final RemoteMap localMap_;
  private final ServerSocket serverSocket_;
  private final ExecutorService workerExecutor_;

  private final Set<Socket> activeClientSockets_ = Collections.newSetFromMap(
      new ConcurrentHashMap<Socket, Boolean>());


  @Inject
  public RemoteMapServer(RemoteMap localMap,
      ServerSocket serverSocket,
      ExecutorService workerExecutor) {
    localMap_ = localMap;
    serverSocket_ = serverSocket;
    workerExecutor_ = workerExecutor;
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

    stop();
    LOGGER.trace("run(): void");
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

  private void stop() {
    try {
      workerExecutor_.shutdownNow();
      shutDownClientSockets();
      workerExecutor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Unexpected interrupt.", e);
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
      try {
        InputStream sis = clientSocket_.getInputStream();
        ObjectInputStream ois = new ObjectInputStream(sis);
        int op = ois.read();
        int type = ois.read();
        String key = (String) ois.readObject();
        if (op == RemoteMap.GET_ID) {
          LOGGER.trace(String.format("get(%d, %s)", type, key));
          ObjectOutputStream oos = new ObjectOutputStream(clientSocket_.getOutputStream());
          oos.writeObject(localMap_.get(type, key));
          oos.flush();
          oos.close();
        } else if (op == RemoteMap.PUT_ID) {
          Serializable value = (Serializable) ois.readObject();
          localMap_.put(type, key, value);
        } else {
          Transaction transaction = (Transaction) ois.readObject();
          localMap_.performTransaction(type, key, transaction);
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
