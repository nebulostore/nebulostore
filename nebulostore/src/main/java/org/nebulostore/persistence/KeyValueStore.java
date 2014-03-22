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

  T get(String key) throws IOException;

  void delete(String key) throws IOException;

  void performTransaction(String key, Function<T, T> function) throws IOException;
}
