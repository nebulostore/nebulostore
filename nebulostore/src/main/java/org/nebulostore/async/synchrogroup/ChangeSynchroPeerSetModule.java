package org.nebulostore.async.synchrogroup;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.messages.AsyncModuleErrorMessage;
import org.nebulostore.async.synchrogroup.messages.AddAsSynchroPeerMessage;
import org.nebulostore.async.synchrogroup.messages.AddedAsSynchroPeerMessage;
import org.nebulostore.async.synchrogroup.messages.RemoveFromSynchroPeerSetMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Module that adds to DHT all synchro-peers from synchroPeersToAdd_ set and removes all
 * synchro-peers from synchroPeersToRemove_ set.
 *
 * @author Szymon Matejczyk
 * @author Piotr Malicki
 *
 */
public class ChangeSynchroPeerSetModule extends ReturningJobModule<Void> {

  private static Logger logger_ = Logger.getLogger(ChangeSynchroPeerSetModule.class);
  private static final long TIME_LIMIT = 10000;

  private final MessageVisitor<Void> visitor_;

  private Set<CommAddress> synchroPeersToAdd_;
  private Set<CommAddress> synchroPeersToRemove_;

  private CommAddress myAddress_;
  private AsyncMessagesContext context_;
  private AppKey appKey_;
  private Timer timer_;

  private final Set<CommAddress> addedPeers_ = new HashSet<>();

  @Inject
  private void setDependencies(CommAddress myAddress, NetworkMonitor networkMonitor,
      AsyncMessagesContext context, AppKey appKey, Timer timer) {
    context_ = context;
    myAddress_ = myAddress;
    appKey_ = appKey;
    timer_ = timer;
  }

  public ChangeSynchroPeerSetModule(Set<CommAddress> synchroPeersToAdd,
      Set<CommAddress> synchroPeersToRemove) {
    synchroPeersToAdd_ = synchroPeersToAdd;
    synchroPeersToRemove_ = synchroPeersToRemove;
    visitor_ = getVisitor();
  }

  protected final CSPSVisitor getVisitor() {
    return new CSPSVisitor();
  }

  private enum State {
    FIRST_PHASE, ADDING_SYNCHRO_PEERS_TO_DHT
  }

  /**
   * Visitor.
   */
  public class CSPSVisitor extends MessageVisitor<Void> {

    private State state_ = State.FIRST_PHASE;
    private Map<CommAddress, Integer> synchroPeerCounters_;

    public Void visit(JobInitMessage message) {
      logger_.info("Starting ChangeSynchroPeerSetModule module with synchro-peers to add: " +
          synchroPeersToAdd_ + "and synchro-peers to remove: " + synchroPeersToRemove_);
      if (context_.isInitialized()) {
        synchroPeersToAdd_ = removePeers(synchroPeersToAdd_, synchroPeersToRemove_);
        synchroPeersToRemove_ = removePeers(synchroPeersToRemove_, synchroPeersToAdd_);

        Map<CommAddress, Integer> synchroPeerCounters = context_.getSynchroPeerCountersCopy();

        /*
         * Update synchro peer counters to be sure that their values in DHT are not lower than
         * values held by synchro-peers in their vector clocks.
         */
        InstanceMetadata metadata = new InstanceMetadata(appKey_);
        for (CommAddress peer : synchroPeersToAdd_) {
          int counter = 0;
          if (synchroPeerCounters.containsKey(peer)) {
            counter = synchroPeerCounters.get(peer) + 1;
          }
          synchroPeerCounters.put(peer, counter);
        }

        Set<CommAddress> synchroGroup = context_.getSynchroGroupForPeerCopy(myAddress_);
        synchroGroup.removeAll(synchroPeersToRemove_);
        metadata.setSynchroPeerCounters(synchroPeerCounters);
        metadata.setSynchroGroup(synchroGroup);
        networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
        synchroPeerCounters_ = synchroPeerCounters;

        /*
         * We can now inform peers from synchroPeersToAdd_ set that we want to add them to our
         * synchro-group.
         */
        if (synchroPeersToAdd_ != null) {
          for (CommAddress synchroPeer : synchroPeersToAdd_) {
            networkQueue_.add(new AddAsSynchroPeerMessage(jobId_, myAddress_, synchroPeer,
                synchroPeerCounters_.get(synchroPeer)));
          }
        }

        timer_.schedule(jobId_, TIME_LIMIT);
      } else {
        logger_.warn("Async messages context has not yet been initialized, ending the module");
        timer_.cancelTimer();
        endWithSuccess(null);
      }
      return null;
    }

    public Void visit(AddedAsSynchroPeerMessage message) {
      logger_.debug("Received " + message.getClass() + " from " + message.getSourceAddress() +
          " in " + ChangeSynchroPeerSetModule.class);
      if (synchroPeersToAdd_.contains(message.getSourceAddress())) {
        addedPeers_.add(message.getSourceAddress());
        if (allPeersAdded() && state_.equals(State.ADDING_SYNCHRO_PEERS_TO_DHT)) {
          logger_.debug("Updating synchro peers set");
          updateSynchroPeers();
        }
      } else {
        logger_.warn("Received " + message + " that was not expected.");
      }
      return null;
    }

    public Void visit(AsyncModuleErrorMessage message) {
      logger_.warn("Received " + message.getClass() + " from " + message.getSourceAddress() +
          " in " + ChangeSynchroPeerSetModule.class);
      synchroPeersToAdd_.remove(message.getSourceAddress());
      if (allPeersAdded() && state_.equals(State.ADDING_SYNCHRO_PEERS_TO_DHT)) {
        updateSynchroPeers();
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout in " + getClass().getCanonicalName() + ". " + addedPeers_.size() +
          " peers were successfully added.");
      if (state_.equals(State.ADDING_SYNCHRO_PEERS_TO_DHT)) {
        logger_.debug("Peers added: " + addedPeers_);
        updateSynchroPeers();
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      if (state_.equals(State.FIRST_PHASE)) {
        logger_.warn("Unable to put new synchro peers' counters and new synchro-group to DHT.");
      } else if (state_.equals(State.ADDING_SYNCHRO_PEERS_TO_DHT)) {
        logger_.warn("Unable to add new synchro-peers to DHT.");
      }
      endWithSuccess(null);
      return null;
    }

    public Void visit(OkDHTMessage message) {
      if (state_.equals(State.FIRST_PHASE)) {
        state_ = State.ADDING_SYNCHRO_PEERS_TO_DHT;
        // synchro-peers removed from DHT, now we can inform them about it
        logger_.debug("Synchro peers removed from DHT: " + synchroPeersToRemove_);
        if (synchroPeersToRemove_ != null) {
          for (CommAddress synchroPeer : synchroPeersToRemove_) {
            networkQueue_.add(new RemoveFromSynchroPeerSetMessage(jobId_, myAddress_, synchroPeer));
          }
        }
        context_.removeAllSynchroPeers(myAddress_, synchroPeersToRemove_);
        context_.updateSynchroPeerCounters(synchroPeerCounters_);

        if (allPeersAdded()) {
          updateSynchroPeers();
        }
      } else if (state_.equals(State.ADDING_SYNCHRO_PEERS_TO_DHT)) {
        logger_.debug("Synchro peers added to DHT: " + addedPeers_);
        context_.addAllSynchroPeers(myAddress_, addedPeers_);
        logger_.debug("Current synchro-group: " + context_.getSynchroGroupForPeerCopy(myAddress_));
        endWithSuccess(null);
      }
      return null;
    }

    private void updateSynchroPeers() {
      timer_.cancelTimer();
      InstanceMetadata metadata = new InstanceMetadata(appKey_);
      Set<CommAddress> synchroGroup = context_.getSynchroGroupForPeerCopy(myAddress_);
      synchroGroup.addAll(addedPeers_);
      metadata.setSynchroGroup(synchroGroup);
      metadata.setSynchroPeerCounters(context_.getSynchroPeerCountersCopy());
      networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
    }

    private boolean allPeersAdded() {
      return synchroPeersToAdd_ == null || addedPeers_.size() == synchroPeersToAdd_.size();
    }

    private Set<CommAddress> removePeers(Set<CommAddress> basePeerSet,
        Set<CommAddress> unnecessaryPeersSet) {
      if (basePeerSet != null) {
        if (unnecessaryPeersSet == null) {
          return basePeerSet;
        } else {
          return Sets.difference(basePeerSet, unnecessaryPeersSet);
        }
      }
      return new HashSet<CommAddress>();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
