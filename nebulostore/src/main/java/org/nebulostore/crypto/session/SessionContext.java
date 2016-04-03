package org.nebulostore.crypto.session;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public class SessionContext {

  private final ReadWriteLock readWriteLock_ = new ReentrantReadWriteLock();
  private final Lock readLock_ = readWriteLock_.readLock();
  private final Lock writeLock_ = readWriteLock_.writeLock();

  private final Map<String, Pair<SessionObject, Integer>> workingSessions_ =
      new HashMap<String, Pair<SessionObject, Integer>>();

  public void acquireWriteLock() {
    writeLock_.lock();
  }

  public void releaseWriteLock() {
    writeLock_.unlock();
  }

  public void acquireReadLock() {
    readLock_.lock();
  }

  public void releaseReadLock() {
    readLock_.unlock();
  }

  public String tryAllocFreeSlot(SessionObject initSessionObject, int ttl) {
    String id = CryptoUtils.getRandomString();
    allocFreeSlot(id, initSessionObject, ttl);
    return id;
  }

  public void allocFreeSlot(String id, SessionObject initSessionObject, int ttl) {
    if (workingSessions_.containsKey(id)) {
      throw new SessionRuntimeException("InitSessionContext already contains " +
          "InitSessionObject for id " + id);
    }
    initSessionObject.setSessionId(id);
    workingSessions_.put(id, new Pair<SessionObject, Integer>(initSessionObject, ttl));
  }

  public SessionObject tryGetInitSessionObject(String id) {
    Pair<SessionObject, Integer> result = workingSessions_.get(id);
    if (result == null) {
      throw new SessionRuntimeException("Unable to get SessionObject for id " + id);
    }
    return result.getFirst();
  }

  public SessionObject tryRemoveInitSessionObject(String id) {
    Pair<SessionObject, Integer> result = workingSessions_.get(id);
    if (result == null) {
      throw new SessionRuntimeException("Unable to remove SessionObject for id " + id);
    }
    if (result.getSecond() == 1) {
      workingSessions_.remove(id);
    } else {
      workingSessions_.put(id, new Pair<SessionObject, Integer>(result.getFirst(),
          result.getSecond() - 1));
    }
    return result.getFirst();
  }
}
