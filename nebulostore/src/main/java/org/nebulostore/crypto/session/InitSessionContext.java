package org.nebulostore.crypto.session;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.crypto.CryptoUtils;

/**
 * @author lukaszsiczek
 */
public class InitSessionContext {

  private final ReadWriteLock readWriteLock_ = new ReentrantReadWriteLock();
  private final Lock readLock_ = readWriteLock_.readLock();
  private final Lock writeLock_ = readWriteLock_.writeLock();

  private final Map<String, InitSessionObject> workingSessions_ =
      new HashMap<String, InitSessionObject>();

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

  public String tryAllocFreeSlot(InitSessionObject initSessionObject) {
    String id = CryptoUtils.getRandomString();
    allocFreeSlot(id, initSessionObject);
    return id;
  }

  public void allocFreeSlot(String id, InitSessionObject initSessionObject) {
    if (workingSessions_.containsKey(id)) {
      throw new SessionRuntimeException("InitSessionContext already contains " +
          "InitSessionObject for id " + id);
    }
    initSessionObject.setSessionId(id);
    workingSessions_.put(id, initSessionObject);
  }

  public InitSessionObject tryGetInitSessionObject(String id) {
    InitSessionObject initSessionObject = workingSessions_.get(id);
    if (initSessionObject == null) {
      throw new SessionRuntimeException("Unable to get InitSessionObject for id " + id);
    }
    return initSessionObject;
  }

  public InitSessionObject tryRemoveInitSessionObject(String id) {
    InitSessionObject initSessionObject = workingSessions_.remove(id);
    if (initSessionObject == null) {
      throw new SessionRuntimeException("Unable to remove InitSessionObject for id " + id);
    }
    return initSessionObject;
  }
}
