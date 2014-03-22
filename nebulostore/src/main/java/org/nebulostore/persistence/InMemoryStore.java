package org.nebulostore.persistence;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Function;

/**
 * Non-persistent storage.
 *
 * @author Bolek Kulbabinski
 */
public class InMemoryStore<T> implements KeyValueStore<T> {

  private final Map<String, T> map_ = new HashMap<>();

  @Override
  public synchronized void put(String key, T value) {
    map_.put(key, value);
  }

  @Override
  public synchronized T get(String key) {
    return map_.get(key);
  }

  @Override
  public synchronized void delete(String key) {
    map_.remove(key);
  }

  @Override
  public synchronized void performTransaction(String key, Function<T, T> function)
      throws IOException {
    map_.put(key, function.apply(map_.get(key)));
  }
}
