package org.nebulostore.async;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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

  private boolean initialized_;

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

  private BlockingQueue<Message> dispatcherQueue_;

  @Inject
  public void setDependencies(CommAddress myAddress,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    myAddress_ = myAddress;
    dispatcherQueue_ = dispatcherQueue;
  }

  /**
   * Initializes inbox holders map with <myAddress_, empty set> as the only pair and clears
   * recipient set.
   */
  public synchronized void initialize() {
    inboxHoldersMap_.clear();
    inboxHoldersMap_.put(myAddress_, new HashSet<CommAddress>());
    recipients_.clear();
    initialized_ = true;
  }

  /**
   * Initializes context with given synchro peer set of the current instance and its recipient set.
   *
   * @param mySynchroPeers
   * @param myRecipients
   */
  public synchronized void
      initialize(Set<CommAddress> mySynchroPeers, Set<CommAddress> myRecipients) {
    inboxHoldersMap_.put(myAddress_, Sets.newHashSet(mySynchroPeers));
    recipients_.addAll(myRecipients);
    initialized_ = true;
  }

  /**
   * Returns boolean value indicating if context has been initialized.
   *
   * @return
   */
  public synchronized boolean isInitialized() {
    return initialized_;
  }

  /**
   * Returns a copy of given peer's synchro group. If the group is not in cache or is not fresh, it
   * will be downloaded from DHT.
   *
   * @param peer
   *          Peer synchro group of which should be returned
   * @return synchro group of peer
   */
  public synchronized Set<CommAddress> getSynchroGroupForPeerCopy(CommAddress peer) {
    // TODO (pm) check if this method does not execute too long
    if (peer.equals(myAddress_)) {
      // Inbox holders set of the current instance is almost always up-to-date.
      return Sets.newHashSet(inboxHoldersMap_.get(myAddress_));
    }
    if (!inboxHoldersMap_.containsKey(peer) || !inboxHoldersTimestamps_.containsKey(peer) ||
        new Date().getTime() - inboxHoldersTimestamps_.get(peer) > REFRESH_TIME_MILIS) {
      ReturningJobModule<Set<CommAddress>> getSynchroPeerSetModule =
          new SynchroPeerSetReturningJobModule(peer);
      dispatcherQueue_.add(new JobInitMessage(getSynchroPeerSetModule));
      try {
        Set<CommAddress> synchroGroup = getSynchroPeerSetModule.getResult(SET_UPDATE_TIMEOUT_MILIS);
        if (synchroGroup != null) {
          inboxHoldersMap_.put(peer, synchroGroup);
          inboxHoldersTimestamps_.put(peer, new Date().getTime());
        }
        return synchroGroup == null ? null : Sets.newHashSet(synchroGroup);
      } catch (NebuloException e) {
        LOGGER.warn("Exception while updating synchro peer set cache: " + e.getMessage());
        return null;
      }
    } else {
      return Sets.newHashSet(inboxHoldersMap_.get(peer));
    }
  }

  /**
   * Check if peer is present in current instance's recipients set.
   *
   * @param peer
   * @return
   */
  public synchronized boolean containsRecipient(CommAddress peer) {
    return recipients_.contains(peer);
  }

  public synchronized void addRecipient(CommAddress peer) {
    recipients_.add(peer);
  }

  /**
   * Add all synchro peers from the synchroPeers set to the set of synchro peers of current
   * instance.
   *
   * @param synchroPeers
   * @param peer
   */
  public synchronized void addAllSynchroPeers(Set<CommAddress> synchroPeers) {
    inboxHoldersMap_.get(myAddress_).addAll(synchroPeers);
  }

  /**
   * Get a copy of the set of recipients.
   *
   * @return set of recipients
   */
  public synchronized Set<CommAddress> getRecipientsCopy() {
    return Sets.newHashSet(recipients_);
  }

  /**
   * Add a message to the set of asynchronous messages for given peer.
   *
   * @param recipient
   *          peer message of which will be added
   * @param message
   *          message to add
   */
  public synchronized void
      addWaitingAsyncMessage(CommAddress recipient, AsynchronousMessage message) {
    if (waitingAsynchronousMessagesMap_.get(recipient) == null) {
      waitingAsynchronousMessagesMap_.put(recipient, new LinkedList<AsynchronousMessage>());
    }
    waitingAsynchronousMessagesMap_.get(recipient).add(message);
  }

  /**
   * Check if addressPair is present in waitingForAck set. If it is not the case, method adds it to
   * the set and returns false. Otherwise it returns true.
   *
   * @param addressPair
   * @return
   */
  public synchronized boolean testAndAddWaitingForAck(Pair<CommAddress, CommAddress> addressPair) {
    if (!waitingForAck_.contains(addressPair)) {
      waitingForAck_.add(addressPair);
      return false;
    }

    return true;
  }

  /**
   * Get a copy of list of waiting asynchronous messages for peer.
   *
   * @param peer
   * @return
   */
  public synchronized List<AsynchronousMessage> getMessagesForPeerListCopy(CommAddress peer) {
    List<AsynchronousMessage> result = waitingAsynchronousMessagesMap_.get(peer);
    if (result == null) {
      return Lists.newLinkedList();
    }
    return Lists.newLinkedList(result);
  }

  /**
   * Remove given entry from the set of entries waiting for ack. Returns true if entry was present
   * in this set.
   *
   * @param entry
   * @return
   */
  public synchronized boolean removeAckWaitingEntry(Pair<CommAddress, CommAddress> entry) {
    return waitingForAck_.remove(entry);
  }

  /**
   * Remove all asynchronous messages sent for given peer. Returns a list of all removed messages.
   *
   * @param peer
   * @return
   */
  public synchronized List<AsynchronousMessage> removeWaitingMessagesForPeer(CommAddress peer) {
    return waitingAsynchronousMessagesMap_.remove(peer);
  }

  /**
   * Store message in the set of asynchronous messages of given peer.
   *
   * @param peer
   * @param message
   */
  public synchronized void storeAsynchronousMessage(CommAddress peer, AsynchronousMessage message) {
    if (!waitingAsynchronousMessagesMap_.containsKey(peer)) {
      waitingAsynchronousMessagesMap_.put(peer, new LinkedList<AsynchronousMessage>());
    }
    waitingAsynchronousMessagesMap_.get(peer).add(message);
  }

}
