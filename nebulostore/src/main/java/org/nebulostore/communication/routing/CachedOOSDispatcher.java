package org.nebulostore.communication.routing;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.inject.Inject;

/**
 * {@link OOSDispatcher} which caches its results for interval time given in constructor.
 *
 * @author Grzegorz Milka
 *
 */
public class CachedOOSDispatcher implements OOSDispatcher {
  private final int cleanTimer_;
  /**
   * Lock for operations on *Sockets_ maps.
   */
  private final Lock socketsLock_ = new ReentrantLock();

  /**
   * Locks associated with given CommAddress.
   *
   * We allow only one connection to given host at the same time.
   */
  private final Map<InetSocketAddress, Lock> socketLocksMap_ = new ConcurrentHashMap<>();

  /**
   * Counts number of users of given CommAddress lock.
   */
  private final Map<InetSocketAddress, Integer> socketLocksWaiters_ = new ConcurrentHashMap<>();
  private final Map<InetSocketAddress, SocketOOSPair> oldSockets_ = new ConcurrentHashMap<>();
  private final Map<InetSocketAddress, SocketOOSPair> ruSockets_ = new ConcurrentHashMap<>();
  private final Map<InetSocketAddress, SocketOOSPair> activeSockets_ = new ConcurrentHashMap<>();

  private Timer socketCleaner_;

  @Inject
  public CachedOOSDispatcher() {
    cleanTimer_ = 4000;
  }

  public CachedOOSDispatcher(int cleanTimer) {
    cleanTimer_ = cleanTimer;
  }

  @Override
  public ObjectOutputStream getStream(InetSocketAddress address) throws InterruptedException,
      IOException {
    boolean isSuccessful = false;
    Lock lock = getLock(address);
    lock.lock();
    try {
      transferSocketIfPresent(address);

      Socket socket = null;
      if (!activeSockets_.containsKey(address)) {
        socket = createSocket(address);
      }

      ObjectOutputStream commOos;
      socketsLock_.lock();
      try {
        if (socket != null) {
          try {
            activeSockets_.put(address, new SocketOOSPair(socket,
                new ObjectOutputStream(socket.getOutputStream())));
          } catch (IOException e) {
            socket.close();
            throw e;
          }
        }
        commOos = activeSockets_.get(address).getOOS();
      } finally {
        socketsLock_.unlock();
      }

      isSuccessful = true;
      return commOos;
    } finally {
      if (!isSuccessful) {
        lock.unlock();
      }
    }

  }

  @Override
  public void putStream(InetSocketAddress address) {
    socketsLock_.lock();
    try {
      ruSockets_.put(address, activeSockets_.get(address));
      activeSockets_.remove(address);
    } finally {
      socketsLock_.unlock();
      Lock lock = socketLocksMap_.get(address);
      releaseLock(address);
      lock.unlock();
    }
  }

  @Override
  public void startUp() {
    /* Set Timer to use a daemon thread */
    socketCleaner_ = new Timer(true);
    socketCleaner_.schedule(new SocketCleaner(), cleanTimer_, cleanTimer_);
  }

  @Override
  public void shutDown() {
    socketCleaner_.cancel();
    socketsLock_.lock();
    try {
      clearAndCloseSocketMap(oldSockets_);
      clearAndCloseSocketMap(ruSockets_);
      clearAndCloseSocketMap(activeSockets_);
    } finally {
      socketsLock_.unlock();
    }
  }

  /**
   * Closes sockets in given socket-OOS Map.
   *
   * Assumes we have appropriate locks to manipulate that map or at least no one
   * else uses those maps.
   * @author Grzegorz Milka
   */
  private void clearAndCloseSocketMap(Map<InetSocketAddress, SocketOOSPair> sockMap) {
    for (SocketOOSPair pair : sockMap.values()) {
      Socket socket = pair.getSocket();
      ObjectOutputStream oos = pair.getOOS();
      try {
        oos.close();
        socket.close();
      } catch (IOException e) {
        /* Ignore */
        oos = null;
      }
    }
    sockMap.clear();
  }

  /**
   * Creates socket to host pointed by commAddress.
   */
  private Socket createSocket(InetSocketAddress inetSocketAddress) throws IOException {
    /* Create socket */
    Socket socket = null;
    try {
      socket = new Socket(inetSocketAddress.getAddress(), inetSocketAddress.getPort());
    } catch (IOException e) {
      throw new IOException("Socket to: " + inetSocketAddress + " could not be created.", e);
    }
    return socket;
  }

  /**
   * Returns lock object for operations to given address.
   *
   * Each getLock should be followed by releaseLock after operations are done.
   * Note: getLock and releaseLock do not lock or unlock the lock in question.
   */
  private synchronized Lock getLock(InetSocketAddress inetAddress) {
    Lock lock = socketLocksMap_.get(inetAddress);
    if (lock == null) {
      lock = new ReentrantLock();
      socketLocksMap_.put(inetAddress, lock);
      socketLocksWaiters_.put(inetAddress, 0);
    }
    int waiters = socketLocksWaiters_.get(inetAddress);
    socketLocksWaiters_.put(inetAddress, waiters + 1);
    return lock;
  }

  /**
   * Decreases the usage count for given lock and removes it from map if no
   * one else has it.
   */
  private synchronized void releaseLock(InetSocketAddress inetAddress) {
    int waiters = socketLocksWaiters_.get(inetAddress);
    if (waiters == 1) {
      socketLocksMap_.remove(inetAddress);
      socketLocksWaiters_.remove(inetAddress);
    } else {
      socketLocksWaiters_.put(inetAddress, waiters - 1);
    }
  }

  /**
   * Transfers socket to address active sockets if it is present in other maps.
   *
   * @param address
   */
  private void transferSocketIfPresent(InetSocketAddress address) {
    socketsLock_.lock();
    try {
      Map<InetSocketAddress, SocketOOSPair> containMap = null;
      if (oldSockets_.containsKey(address)) {
        containMap = oldSockets_;
      } else if (ruSockets_.containsKey(address)) {
        containMap = ruSockets_;
      }

      if (containMap != null) {
        activeSockets_.put(address, containMap.get(address));
        containMap.remove(address);
      }
    } finally {
      socketsLock_.unlock();
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private class SocketCleaner extends TimerTask {
    @Override
    public void run() {
      socketsLock_.lock();
      try {
        clearAndCloseSocketMap(oldSockets_);
        oldSockets_.putAll(ruSockets_);
        ruSockets_.clear();
      } finally {
        socketsLock_.unlock();
      }
    }
  }

  /**
   * Structure for combining socket and ObjectOutputStream with it. ObjectOutputStream follows a
   * semantics of sending some kind of initializing message when opening it. Therefore we can't
   * recreate it every time we want without doing same on the receiver side (or doing some tricks).
   * It is cheaper to simply let it be.
   *
   * @author Grzegorz Milka
   */
  private static class SocketOOSPair {
    private Socket socket_;
    private ObjectOutputStream oos_;

    public SocketOOSPair(Socket newSocket, ObjectOutputStream newOos) {
      socket_ = newSocket;
      oos_ = newOos;
    }

    public ObjectOutputStream getOOS() {
      return oos_;
    }

    public Socket getSocket() {
      return socket_;
    }
  }

}



