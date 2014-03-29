package org.nebulostore.replicaresolver;

import java.io.IOException;

import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * Map for contracts.
 *
 * @author Grzegorz Milka
 *
 */
public interface ReplicaResolver {
  /**
   * Gives current ValueDHT for given key.
   *
   * @return null if no mapping exists.
   * @throws IOException
   */
  ValueDHT get(KeyDHT key) throws IOException;

  /**
   * Puts given mapping to Contract database.
   *
   * @throws IOException
   */
  void put(KeyDHT key, ValueDHT value) throws IOException;
}
