package org.nebulostore.crypto.session;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionContext {

  private final ReadWriteLock readWriteLock_ = new ReentrantReadWriteLock();
  private final Lock readLock_ = readWriteLock_.readLock();
  private final Lock writeLock_ = readWriteLock_.writeLock();

  private final Map<CommAddress, InitSessionObject> workingSessions_ =
      new HashMap<CommAddress, InitSessionObject>();

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

  public void tryAllocFreeSlot(CommAddress commAddress, InitSessionObject initSessionObject) {
    if (workingSessions_.containsKey(commAddress)) {
      throw new SessionRuntimeException("InitSessionContext already contains " +
          "InitSessionObject for peer " + commAddress);
    }
    workingSessions_.put(commAddress, initSessionObject);
  }

  public InitSessionObject tryGetInitSessionObject(CommAddress peerAddress) {
    InitSessionObject initSessionObject = workingSessions_.get(peerAddress);
    if (initSessionObject == null) {
      throw new SessionRuntimeException("Unable to get InitSessionObject for peer " + peerAddress);
    }
    return initSessionObject;
  }

  public InitSessionObject tryRemoveInitSessionObject(CommAddress peerAddress) {
    InitSessionObject initSessionObject = workingSessions_.remove(peerAddress);
    if (initSessionObject == null) {
      throw new SessionRuntimeException("Unable to remove InitSessionObject for peer " +
          peerAddress);
    }
    return initSessionObject;
  }
}
