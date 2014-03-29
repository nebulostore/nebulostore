package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.Remote;

/**
 * Remote Map interface.
 *
 * @author Grzegorz Milka
 *
 */
public interface RemoteMap extends Remote {
  int GET_ID = 0;
  int PUT_ID = 1;
  int TRAN_ID = 2;

  /**
   * Returns value to which specified key is mapped.
   *
   * @return null if no mapping exists.
   * @throws IOException
   */
  Serializable get(int type, String key) throws IOException;

  /**
   * Puts given mapping to map.
   *
   * @throws IOException
   */
  void put(int type, String key, Serializable value) throws IOException;

  void performTransaction(int type, String key, Transaction transaction) throws IOException;
}
