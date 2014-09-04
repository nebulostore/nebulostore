package org.nebulostore.async;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.utils.Pair;

/**
 * Persistent data shared by parts of asynchronous messages module.
 *
 * @author Piotr Malicki
 *
 */
public final class AsyncMessagesContext {

  private static final Logger LOGGER = Logger.getLogger(AsyncMessagesContext.class);
  private static final Long REFRESH_TIME_MILIS = 5000L;
  private static final int SET_UPDATE_TIMEOUT_MILIS = 1000;

  /**
   * Messages waiting to be retrieved by peers.
   */
  private final Map<CommAddress, List<AsynchronousMessage>> waitingAsynchronousMessagesMap_ =
      new HashMap<>();

  /**
   * Pair of CommAddresses. First element in the pair denotes peer that asked for AM but haven't
   * sent response yet. The second one denotes address of peer to which AMs were sent.
   */
  private final Set<Pair<CommAddress, CommAddress>> waitingForAck_ = new HashSet<>();

  /**
   * Synchro-groups to which current instance belongs. Cached from DHT. Synchro-group owned by
   * current peer is most of the time up-to-date, the rest is updated when needed.
   */
  private final Map<CommAddress, Set<CommAddress>> inboxHoldersMap_ = new HashMap<>();

  /**
   * All peers for which asynchronous messages are stored at current instance.
   */
  private final Set<CommAddress> recipients_ = new HashSet<>();

  /**
   * Map with timestamps indicating the moment of last update of synchro peer list for given peer's
   * communication address.
   */
  private final Map<CommAddress, Long> inboxHoldersTimestamps_ = new HashMap<>();

  private CommAddress myAddress_;

  private final ReadWriteLock inboxHoldersReadWriteLock_ = new ReentrantReadWriteLock();
  private final Lock inboxHoldersReadLock_ = inboxHoldersReadWriteLock_.readLock();
  private final Lock inboxHoldersWriteLock_ = inboxHoldersReadWriteLock_.writeLock();

  private final ReadWriteLock messagesReadWriteLock_ = new ReentrantReadWriteLock();
  private final Lock messagesReadLock_ = messagesReadWriteLock_.readLock();
  private final Lock messagesWriteLock_ = messagesReadWriteLock_.writeLock();

  /**
   * JobId of jobs we are waiting for response from.
   */
  private final Map<String, Set<String>> waitingForMessages_ = new ConcurrentHashMap<>();

  private BlockingQueue<Message> dispatcherQueue_;

  @Inject
  public void setDependencies(CommAddress myAddress,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    myAddress_ = myAddress;
    dispatcherQueue_ = dispatcherQueue;
  }

  /**
   * Initializes inbox holders map with <myAddress_, empty set> as the only pair and clears
   * recipient set. Use this method with inbox holders map write rights acquired.
   */
  public void initialize() {
    inboxHoldersMap_.clear();
    inboxHoldersMap_.put(myAddress_, new HashSet<CommAddress>());
    recipients_.clear();
  }

  /**
   * Returns synchro group of given peer. If the group is not in cache or is not fresh, it will be
   * downloaded from DHT. Inbox holders write rights are needed when calling this method.
   *
   * @param peer
   *          Peer synchro group of which should be returned
   * @return synchro group of peer
   */
  public Set<CommAddress> getSynchroGroupForPeer(CommAddress peer) {
    // TODO (pm) check if this method does not execute too long
    if (peer.equals(myAddress_)) {
      // Inbox holders set of the current instance is almost always up-to-date.
      return inboxHoldersMap_.get(myAddress_);
    }
    if (!inboxHoldersMap_.containsKey(peer) || !inboxHoldersTimestamps_.containsKey(peer) ||
        new Date().getTime() - inboxHoldersTimestamps_.get(peer) > REFRESH_TIME_MILIS) {
      ReturningJobModule<Set<CommAddress>> getSynchroPeerSetModule =
          new SynchroPeerSetReturningJobModule(peer);
      dispatcherQueue_.add(new JobInitMessage(getSynchroPeerSetModule));
      try {
        Set<CommAddress> synchroGroup = getSynchroPeerSetModule
            .getResult(SET_UPDATE_TIMEOUT_MILIS);
        inboxHoldersMap_.put(peer, synchroGroup);
        inboxHoldersTimestamps_.put(peer, new Date().getTime());
        return synchroGroup;
      } catch (NebuloException e) {
        LOGGER.warn("Exception while updating synchro peer set cache: " + e.getMessage());
        return null;
      }
    } else {
      return inboxHoldersMap_.get(peer);
    }
  }

  public void acquireInboxHoldersReadRights() {
    inboxHoldersReadLock_.lock();
    LOGGER.debug("Read rights for inbox holders map acquired.");
  }

  public void freeInboxHoldersReadRights() {
    inboxHoldersReadLock_.unlock();
    LOGGER.debug("Read rights for inbox holders map freed.");
  }

  public void acquireInboxHoldersWriteRights() {
    inboxHoldersWriteLock_.lock();
    LOGGER.debug("Write rights for inbox holders map acquired.");

  }

  public void freeInboxHoldersWriteRights() {
    inboxHoldersWriteLock_.unlock();
    LOGGER.debug("Write rights for inbox holders map freed.");
  }

  /**
   * Get the set of recipients. You should have at least read lock for inbox holders
   * acquired before calling this method.
   *
   * @return set of recipients
   */
  public Set<CommAddress> getRecipients() {
    return recipients_;
  }

  /**
   * Add a message to the set of asynchronous messages for given peer. Write rights for messages are
   * required when calling this method.
   *
   * @param recipient
   *          peer message of which will be added
   * @param message
   *          message to add
   */
  public void addWaitingAsyncMessage(CommAddress recipient, AsynchronousMessage message) {
    if (waitingAsynchronousMessagesMap_.get(recipient) == null) {
      waitingAsynchronousMessagesMap_.put(recipient, new LinkedList<AsynchronousMessage>());
    }
    waitingAsynchronousMessagesMap_.get(recipient).add(message);
  }

  public void acquireMessagesReadRights() {
    messagesReadLock_.lock();
  }

  public void freeMessagesReadRights() {
    messagesReadLock_.unlock();
  }

  public void acquireMessagesWriteRights() {
    messagesWriteLock_.lock();
  }

  public void freeMessagesWriteRights() {
    messagesWriteLock_.unlock();
  }

  /**
   * Get waiting messages map. This method assumes that read or write lock for messages is acquired.
   *
   * @return waiting messages map
   */
  public Map<CommAddress, List<AsynchronousMessage>> getWaitingAsynchronousMessagesMap() {
    return waitingAsynchronousMessagesMap_;
  }

  /**
   * Get the whole set of peers from which we are waiting for ack message. Read or write rights are
   * needed when calling this method.
   *
   * @return the whole set of peers from which we are waiting for ack message
   */
  public Set<Pair<CommAddress, CommAddress>> getWaitingForAck() {
    return waitingForAck_;
  }

  /**
   * Get job ids of modules from which we are waiting for messages and were started by the module
   * with identifier jobId.
   *
   * @param jobId
   *          Job Id of asking module
   * @return set of job ids
   */
  public Set<String> getWaitingForMessages(String jobId) {
    Set<String> waitingForMessages = waitingForMessages_.get(jobId);
    if (waitingForMessages == null) {
      waitingForMessages = new ConcurrentSkipListSet<>();
      waitingForMessages_.put(jobId, waitingForMessages);
    }
    return waitingForMessages;
  }

}
