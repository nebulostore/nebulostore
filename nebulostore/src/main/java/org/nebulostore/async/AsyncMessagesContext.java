package org.nebulostore.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.synchronization.VectorClockValue;
import org.nebulostore.async.util.RecipientPeerData;
import org.nebulostore.async.util.RecipientsData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

/**
 * Persistent data shared by parts of asynchronous messages module.
 *
 * @author Piotr Malicki
 *
 */
public final class AsyncMessagesContext {

  private static Logger logger_ = Logger.getLogger(AsyncMessagesContext.class);

  private static final int INITIAL_RECIPIENT_FRESHNESS = 1;
  private static final int MAX_RECIPIENTS_NUMBER = 7;

  private boolean initialized_;

  /**
   * Map with waiting asynchronous messages for each synchro group current instance belongs to.
   */
  private final Map<CommAddress, Set<AsynchronousMessage>> waitingMessages_ = new HashMap<>();

  /**
   * Map with timestamp maps for each synchro group current instance belongs to. Map for synchro
   * group owner <i>peer</i> contains <message, timestamp> pairs for messages in
   * waitingMessages_.get(peer).
   */
  private final Map<CommAddress, Map<String, VectorClockValue>> messagesTimestamps_ =
      new HashMap<>();

  /**
   * Map with messages set's last clear timestamps for each synchro group current instance belongs
   * to.
   */
  private final Map<CommAddress, VectorClockValue> setClearTimestamps_ = new HashMap<>();

  /**
   * Synchro-groups to which current instance belongs. Cached from DHT. Synchro-group owned by
   * current peer is most of the time up-to-date, the rest is updated periodically.
   */
  private final Map<CommAddress, Set<CommAddress>> inboxHoldersMap_ = new HashMap<>();

  /**
   * Map with vector clocks for each synchro group current instance belongs to.
   */
  private final Map<CommAddress, VectorClockValue> synchroClocks_ = new HashMap<>();

  /**
   * All peers for which asynchronous messages are stored at current instance.
   */
  private final Set<CommAddress> recipients_ = new HashSet<>();

  /**
   * Freshnesses of recipients. Fresshness counter is decremented each time when synchro group cache
   * is refreshed and its value is bigger then 0. When freshness equals 0 we assume that recipient
   * has been already added to synchro group in DHT. If it's not the case, we remove him.
   */
  private final Map<CommAddress, Integer> recipientsFreshnesses_ = new HashMap<>();

  /**
   * Counters indicating how many times each peer was added as a synchro peer of current instance.
   */
  private final Map<CommAddress, Integer> synchroPeerCounters_ = new HashMap<>();

  /**
   * Number of current version of recipients set.
   */
  private int recipientsSetVersion_;

  /**
   * Set containing addresses of peers which we are currently adding or removing from the recipients
   * set.
   */
  private final Set<CommAddress> recipientsChangesLocks_ = new HashSet<>();

  /**
   * Set containing addresses of owners of groups for which we are currently retrieving messages.
   */
  private final Set<CommAddress> groupLocks_ = new HashSet<>();

  private final CommAddress myAddress_;

  @Inject
  public AsyncMessagesContext(CommAddress myAddress) {
    myAddress_ = myAddress;
  }

  /**
   * Initializes all structures with basic info.
   */
  public synchronized void initialize() {
    inboxHoldersMap_.clear();
    inboxHoldersMap_.put(myAddress_, new HashSet<CommAddress>());
    waitingMessages_.put(myAddress_, new HashSet<AsynchronousMessage>());
    messagesTimestamps_.put(myAddress_, new HashMap<String, VectorClockValue>());
    setClearTimestamps_.put(myAddress_, new VectorClockValue(myAddress_, 0));
    synchroClocks_.put(myAddress_, new VectorClockValue(myAddress_, 0));
    recipients_.clear();
    recipientsChangesLocks_.clear();
    recipientsFreshnesses_.clear();
    synchroPeerCounters_.clear();
    initialized_ = true;
  }

  /**
   * Initializes context with given data.
   *
   * @param mySynchroPeers
   * @param myRecipients
   */
  public synchronized void initialize(Set<CommAddress> mySynchroPeers,
      Set<CommAddress> myRecipients, int recipientsSetVersion,
      Map<CommAddress, Integer> recipientsCounters) {
    Set<CommAddress> synchroGroup;
    if (mySynchroPeers == null) {
      synchroGroup = new HashSet<>();
    } else {
      synchroGroup = Sets.newHashSet(mySynchroPeers);
    }
    inboxHoldersMap_.put(myAddress_, synchroGroup);
    if (myRecipients != null) {
      recipients_.addAll(myRecipients);
    }
    recipientsSetVersion_ = recipientsSetVersion;
    recipientsChangesLocks_.clear();
    // TODO (pm) Shouldn't this be remembered in DHT?
    for (CommAddress recipient : recipients_) {
      recipientsFreshnesses_.put(recipient, 0);
    }

    if (recipientsCounters != null) {
      synchroPeerCounters_.clear();
      synchroPeerCounters_.putAll(recipientsCounters);
    }

    recipients_.add(myAddress_);
    for (CommAddress peer : recipients_) {
      synchroClocks_.put(peer, new VectorClockValue(myAddress_, 0));
      waitingMessages_.put(peer, new HashSet<AsynchronousMessage>());
      messagesTimestamps_.put(peer, new HashMap<String, VectorClockValue>());
      setClearTimestamps_.put(peer, new VectorClockValue(myAddress_, 0));
    }

    recipients_.remove(myAddress_);
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
   * Returns a copy of given peer's synchro group. Returns null if the group is not in cache.
   *
   * @param peer
   *          Peer synchro group of which should be returned
   * @return synchro group of peer
   */
  public synchronized Set<CommAddress> getSynchroGroupForPeerCopy(CommAddress peer) {
    if (inboxHoldersMap_.get(peer) != null) {
      return Sets.newHashSet(inboxHoldersMap_.get(peer));
    }
    return null;
  }

  public synchronized Map<CommAddress, Integer> getSynchroPeerCountersCopy() {
    return Maps.newHashMap(synchroPeerCounters_);
  }

  /**
   * Update synchro peer counter map with entries from the argument.
   *
   * @param synchroPeerCounters
   */
  public synchronized void updateSynchroPeerCounters(
      Map<CommAddress, Integer> synchroPeerCounters) {
    for (CommAddress peer : synchroPeerCounters.keySet()) {
      synchroPeerCounters_.put(peer, synchroPeerCounters.get(peer));
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

  /**
   * Add a recipient to the recipients set of current instance.
   *
   * @param peer
   */
  public synchronized boolean addRecipient(CommAddress peer, int counterValue) {
    if (recipients_.size() < MAX_RECIPIENTS_NUMBER) {
      recipients_.add(peer);
      recipientsSetVersion_++;
      recipientsFreshnesses_.put(peer, INITIAL_RECIPIENT_FRESHNESS);
      synchroClocks_.put(peer, new VectorClockValue(myAddress_, counterValue));
      waitingMessages_.put(peer, new HashSet<AsynchronousMessage>());
      messagesTimestamps_.put(peer, new HashMap<String, VectorClockValue>());
      inboxHoldersMap_.put(peer, new HashSet<CommAddress>());
      inboxHoldersMap_.get(peer).add(myAddress_);
      setClearTimestamps_.put(peer, new VectorClockValue(myAddress_, counterValue));
      return true;
    }
    return false;
  }

  /**
   * Remove recipient and all the data connected with him excluding his synchro-peer set.
   *
   * @param recipient
   */
  public synchronized void removeRecipient(CommAddress recipient) {
    removeRecipientPartially(recipient);
    inboxHoldersMap_.remove(recipient);
  }

  public synchronized boolean lockRecipient(CommAddress peer) {
    if (recipientsChangesLocks_.contains(peer)) {
      return false;
    }

    recipientsChangesLocks_.add(peer);
    return true;
  }

  public synchronized void freeRecipient(CommAddress peer) {
    recipientsChangesLocks_.remove(peer);
  }

  public synchronized int getRecipientsSetVersion() {
    return recipientsSetVersion_;
  }

  public synchronized VectorClockValue getCurrentClockValueCopy(CommAddress peer) {
    return synchroClocks_.get(peer).getValueCopy();
  }

  /**
   * Add all synchro peers from the synchroPeers set to the set of synchro peers of peer argument.
   *
   * @param synchroPeers
   * @param peer
   */
  public synchronized void addAllSynchroPeers(CommAddress peer, Set<CommAddress> synchroPeers) {
    inboxHoldersMap_.get(peer).addAll(synchroPeers);
    if (peer.equals(myAddress_)) {
      for (CommAddress synchroPeer : synchroPeers) {
        if (synchroPeerCounters_.containsKey(synchroPeer)) {
          synchroPeerCounters_.put(synchroPeer, synchroPeerCounters_.get(synchroPeer) + 1);
        } else {
          synchroPeerCounters_.put(synchroPeer, 0);
        }
      }
    }
  }

  /**
   * Remove all synchro peers in the synchroPeers set from the set of synchro peers of peer
   * argument.
   *
   * @param synchroPeers
   * @param peer
   */
  public synchronized void removeAllSynchroPeers(CommAddress peer, Set<CommAddress> synchroPeers) {
    inboxHoldersMap_.get(peer).removeAll(synchroPeers);
  }

  /**
   * Clear the cached synchro-group of peer and fill it with addresses from synchroGroup set.
   *
   * @param peer
   * @param synchroGroup
   */
  public synchronized void updateSynchroGroup(CommAddress peer, Set<CommAddress> synchroGroup) {
    if (inboxHoldersMap_.containsKey(peer)) {
      inboxHoldersMap_.get(peer).clear();

      if (synchroGroup != null) {
        inboxHoldersMap_.get(peer).addAll(synchroGroup);
      }
    }
  }
  /**
   * Get a copy of the recipients data.
   *
   * @param peer
   * @return
   */
  public synchronized RecipientsData getRecipientsData() {
    return new RecipientsData(Sets.newHashSet(recipients_), recipientsSetVersion_);
  }

  /**
   * Remove all synchro-groups to which current instance does not belong.
   */
  public synchronized void removeUnnecessarySynchroGroups() {

    for (Iterator<Entry<CommAddress, Set<CommAddress>>> iterator =
        inboxHoldersMap_.entrySet().iterator(); iterator.hasNext();) {
      Entry<CommAddress, Set<CommAddress>> entry = iterator.next();
      if (!entry.getKey().equals(myAddress_)) {
        int currentFreshness = recipientsFreshnesses_.get(entry.getKey());
        if (currentFreshness == 0) {
          if (!entry.getValue().contains(myAddress_)) {
            logger_.debug("Removing " + entry.getKey() + " from recipients set");
            removeRecipientPartially(entry.getKey());
            iterator.remove();
          }
        } else {
          recipientsFreshnesses_.put(entry.getKey(), currentFreshness - 1);
        }
      }
    }
  }
  /**
   * Get all data connected with recipient peer stored in the context.
   *
   * @param peer
   * @return
   */
  public synchronized RecipientPeerData getRecipientPeerDataForPeer(CommAddress peer) {
    VectorClockValue clearTimestamp = setClearTimestamps_.get(peer);
    if (clearTimestamp == null) {
      clearTimestamp = new VectorClockValue(myAddress_, 0);
    }

    Set<AsynchronousMessage> messages = waitingMessages_.get(peer);
    if (messages == null) {
      messages = new HashSet<AsynchronousMessage>();
    }

    Map<String, VectorClockValue> timestamps = messagesTimestamps_.get(peer);
    if (timestamps == null) {
      timestamps = new HashMap<String, VectorClockValue>();
    }

    Map<String, VectorClockValue> msgTimestamps = new HashMap<>();
    for (Entry<String, VectorClockValue> entry : timestamps.entrySet()) {
      msgTimestamps.put(entry.getKey(), entry.getValue().getValueCopy());
    }

    VectorClockValue clockValue = synchroClocks_.get(peer);
    if (clockValue != null) {
      clockValue = clockValue.getValueCopy();
    }

    return new RecipientPeerData(Sets.newHashSet(messages), clearTimestamp.getValueCopy(),
        msgTimestamps, clockValue, recipients_.contains(peer));
  }

  /**
   * Update all data connected with recipient using values given as arguments of the method.
   *
   * @param recipient
   * @param messages
   * @param messagesTimestamps
   * @param lastClearTimestamp
   * @param clockValue
   */
  public synchronized void updateRecipientDataForPeer(CommAddress recipient,
      Set<AsynchronousMessage> messages, Map<String, VectorClockValue> messagesTimestamps,
      VectorClockValue lastClearTimestamp, VectorClockValue clockValue) {
    logger_.debug("Updating recipient data for peer: " + recipient + "\n messages: " + messages +
        "\n messagesTimestamps: " + messagesTimestamps + "\n lastClearTimestamp: " +
        lastClearTimestamp + "\n clockValue: " + clockValue + "\n");

    if (recipients_.contains(recipient) || recipient.equals(myAddress_)) {

      logger_.debug("Current values: \n" + "messages: " + waitingMessages_.get(recipient) +
          "\n messagesTimestamps: " + messagesTimestamps_.get(recipient) +
          "\n lastClearTimestamp: " + setClearTimestamps_.get(recipient) + "\n clockValue: " +
          synchroClocks_.get(recipient));

      if (setClearTimestamps_.get(recipient).areAllElementsLowerEqual(lastClearTimestamp)) {
        setClearTimestamps_.put(recipient, lastClearTimestamp);
      }
      VectorClockValue currentClearTime = setClearTimestamps_.get(recipient);

      removeMessagesFromBeforeTimestamp(waitingMessages_.get(recipient),
          messagesTimestamps_.get(recipient), currentClearTime);
      removeMessagesFromBeforeTimestamp(messages, messagesTimestamps, currentClearTime);

      Set<AsynchronousMessage> waitingMsgs = waitingMessages_.get(recipient);
      for (AsynchronousMessage message : messages) {
        VectorClockValue timestamp = messagesTimestamps_.get(recipient).get(message.getMessageId());
        VectorClockValue receivedTimestamp = messagesTimestamps.get(message.getMessageId());
        if (!waitingMsgs.contains(message) ||
            (timestamp != null && timestamp.compareTo(receivedTimestamp) >= 0)) {
          waitingMsgs.add(message);
        }
        messagesTimestamps_.get(recipient).put(message.getMessageId(), receivedTimestamp);
      }

      synchroClocks_.get(recipient).updateClockValue(clockValue);

      logger_.debug("New values: \n" + "messages: " + waitingMessages_.get(recipient) +
          "\n messagesTimestamps: " + messagesTimestamps_.get(recipient) +
          "\n lastClearTimestamp: " + setClearTimestamps_.get(recipient) + "\n clockValue: " +
          synchroClocks_.get(recipient));

    } else {
      logger_.info("Peer " + recipient + " is not present in recipients set, updating messages " +
          "for this peer failed.");
    }
  }

  /**
   * Remove all asynchronous messages sent for given peer and set timestamp to current timer value.
   * Returns a set of all removed messages paired with corresponding timestamps.
   *
   * @param peer
   * @return
   */
  public synchronized Set<Pair<AsynchronousMessage, VectorClockValue>> clearWaitingMessagesForPeer(
      CommAddress peer) {
    logger_.debug("Clearing waiting messages: " + waitingMessages_.get(peer) + " recipient: " +
        peer);
    synchroClocks_.get(myAddress_).tick();
    setClearTimestamps_.put(peer, synchroClocks_.get(myAddress_).getValueCopy());
    Set<AsynchronousMessage> messages = waitingMessages_.remove(peer);
    waitingMessages_.put(peer, new HashSet<AsynchronousMessage>());

    Map<String, VectorClockValue> timestamps = messagesTimestamps_.remove(peer);
    messagesTimestamps_.put(peer, new HashMap<String, VectorClockValue>());

    Set<Pair<AsynchronousMessage, VectorClockValue>> result = new HashSet<>();
    if (messages != null) {
      for (AsynchronousMessage message : messages) {
        result.add(new Pair<AsynchronousMessage, VectorClockValue>(message, timestamps.get(message
            .getMessageId())));
      }
    }
    return result;
  }

  /**
   * Store message in the set of asynchronous messages of given peer.
   *
   * @param peer
   * @param message
   */
  public synchronized void storeAsynchronousMessage(CommAddress peer, AsynchronousMessage message) {
    synchroClocks_.get(peer).tick();
    VectorClockValue messageTimestamp = synchroClocks_.get(peer).getValueCopy();
    if (recipients_.contains(peer)) {
      logger_.debug("Storing message: " + message + " for peer: " + peer + " with timestamp " +
          messageTimestamp);
      if (waitingMessages_.get(peer) == null) {
        waitingMessages_.put(peer, new HashSet<AsynchronousMessage>());
      }

      Set<AsynchronousMessage> messages = waitingMessages_.get(peer);
      if (messages.contains(message)) {
        // message already in set, select lower timestamp
        VectorClockValue currentTimestamp =
            messagesTimestamps_.get(peer).get(message.getMessageId());
        if (messageTimestamp.compareTo(currentTimestamp) < 0) {
          messagesTimestamps_.get(peer)
              .put(message.getMessageId(), messageTimestamp.getValueCopy());
        }
      } else {
        messages.add(message);
        messagesTimestamps_.get(peer).put(message.getMessageId(), messageTimestamp.getValueCopy());
      }
    } else {
      logger_.info("Peer " + peer + " is not present in recipients set, message won't be stored");
    }
  }

  private void removeMessagesFromBeforeTimestamp(Set<AsynchronousMessage> messages,
      Map<String, VectorClockValue> messagesTimestamps, VectorClockValue timestamp) {
    Iterator<AsynchronousMessage> iterator = messages.iterator();
    while (iterator.hasNext()) {
      AsynchronousMessage message = iterator.next();
      VectorClockValue msgTimestamp = messagesTimestamps.get(message.getMessageId());
      if (msgTimestamp == null || msgTimestamp.areAllElementsLowerEqual(timestamp)) {
        messagesTimestamps.remove(message.getMessageId());
        iterator.remove();
      }
    }
  }

  /**
   * Remove recipient and all the data connected with him excluding his synchro-peer set.
   *
   * @param recipient
   */
  private void removeRecipientPartially(CommAddress recipient) {
    recipients_.remove(recipient);
    recipientsFreshnesses_.remove(recipient);
    waitingMessages_.remove(recipient);
    synchroClocks_.remove(recipient);
    setClearTimestamps_.remove(recipient);
    recipientsSetVersion_++;
  }

  public synchronized boolean tryLockGroup(CommAddress address) {
    if (groupLocks_.contains(address)) {
      return false;
    }
    groupLocks_.add(address);
    return true;
  }

  public synchronized void unlockGroup(CommAddress address) {
    groupLocks_.remove(address);
  }
}
