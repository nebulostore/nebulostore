package org.nebulostore.persistence;

import java.io.IOException;

import com.google.common.base.Function;

/**
 * Interface for any persistent key/value store. All methods are thread-safe and atomic.
 *
 * @author Bolek Kulbabinski
 */
public interface KeyValueStore<T> {

  void put(String key, T value) throws IOException;

  /**
   * Return object or null if key not found.
   */
  T get(String key);

  void delete(String key) throws IOException;

  /**
   * Atomically apply function to an object stored under given key.
   */
  void performTransaction(String key, Function<T, T> function) throws IOException;
}
