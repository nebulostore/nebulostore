package org.nebulostore.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.base.Preconditions;

public class LockMap {
  private final Map<String, Lock> locksMap_ = new HashMap<>();
  private final Map<String, Integer> locksUsersCount_ = new HashMap<>();

  /**
   * Store locks that are used.
   */
  public void lock(String key) {
    Lock lock = null;
    synchronized (locksMap_) {
      lock = locksMap_.get(key);
      if (lock == null) {
        lock = new ReentrantLock();
        locksMap_.put(key, lock);
        locksUsersCount_.put(key, 1);
      } else {
        locksUsersCount_.put(key, locksUsersCount_.get(key) + 1);
      }
    }
    lock.lock();
  }

  /**
   * Remove unused locks to avoid memory leaks.
   */
  public void unlock(String key) {
    synchronized (locksMap_) {
      Lock lock = locksMap_.get(key);
      Preconditions.checkNotNull(lock, "Lock for key " + key + " does not exist in locksMap");
      lock.unlock();
      Integer users = locksUsersCount_.get(key);
      if (users == 1) {
        locksMap_.remove(key);
        locksUsersCount_.remove(key);
      } else {
        locksUsersCount_.put(key, users - 1);
      }
    }
  }
}
