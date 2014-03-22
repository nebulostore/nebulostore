package org.nebulostore.newcommunication.netutils.remotemap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.log4j.Logger;

/**
 * @author Grzegorz Milka
 */
public class RemoteMapClient implements RemoteMap {
  private static final Logger LOGGER = Logger.getLogger(RemoteMapClient.class);
  private static final int N_TRIES = 2;
  private final InetSocketAddress serverAddress_;

  public RemoteMapClient(InetSocketAddress serverAddress) {
    serverAddress_ = serverAddress;
  }

  @Override
  public Serializable get(int type, String key) throws IOException {
    for (int i = 1; i < N_TRIES; ++i) {
      try {
        return getOnce(type, key);
      } catch (IOException e) {
        LOGGER.trace(String.format("get(%d, %s)", type, key), e);
      }
    }
    return getOnce(type, key);
  }

  @Override
  public void performTransaction(int type, String key, Transaction transaction)
      throws IOException {
    LOGGER.debug(String.format("performTransaction(%d,%s, %s)", type, key, transaction));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      try {
        oos.write(TRAN_ID);
        oos.write(type);
        oos.writeObject(key);
        oos.writeObject(transaction);
      } finally {
        oos.flush();
        oos.close();
      }
    }
  }

  @Override
  public void put(int type, String key, Serializable value) throws IOException {
    LOGGER.debug(String.format("put(%d,%s, %s)", type, key, value));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      try {
        oos.write(PUT_ID);
        oos.write(type);
        oos.writeObject(key);
        oos.writeObject(value);
      } finally {
        oos.flush();
        oos.close();
      }
    }
  }

  private Serializable getOnce(int type, Serializable key) throws IOException {
    LOGGER.trace(String.format("get(%d, %s)", type, key));
    try (Socket socket = new Socket(serverAddress_.getAddress(), serverAddress_.getPort())) {
      ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
      oos.write(GET_ID);
      oos.write(type);
      oos.writeObject(key);

      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      try {
        Serializable value = (Serializable) ois.readObject();
        LOGGER.trace(String.format("get(%d, %s): %s", type, key, value));
        return value;
      } catch (ClassNotFoundException e) {
        LOGGER.warn(String.format("get(%d,%s): null", type, key), e);
        return null;
      }
    } catch (IOException e) {
      throw e;
    }
  }

}
