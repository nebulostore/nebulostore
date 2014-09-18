package org.nebulostore.async;

import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AddAsSynchroPeerMessage;
import org.nebulostore.async.messages.AddedAsSynchroPeerMessage;
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
 * @author sm262956
 * @author Piotr Malicki
 *
 */
public class ChangeSynchroPeerSetModule extends JobModule {
  // TODO (pm) Consistency of data with DHT

  private static Logger logger_ = Logger.getLogger(ChangeSynchroPeerSetModule.class);
  private static final int TIME_LIMIT = 3000;

  private final MessageVisitor<Void> visitor_;

  private final Set<CommAddress> synchroPeersToAdd_;
  private final Set<CommAddress> synchroPeersToRemove_;

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

  protected CSPSVisitor getVisitor() {
    return new CSPSVisitor();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Visitor.
   */
  public class CSPSVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) {
      logger_.info("Starting ChangeSynchroPeerSetModule module with synchro-peers to add: " +
          synchroPeersToAdd_ + "and synchro-peers to remove: " + synchroPeersToRemove_);
      if (context_.isInitialized()) {
        jobId_ = message.getId();
        if (synchroPeersToAdd_ != null) {
          for (CommAddress synchroPeer : synchroPeersToAdd_) {
            networkQueue_.add(new AddAsSynchroPeerMessage(jobId_, myAddress_, synchroPeer));
          }
        }

        // TODO (pm) remove peers from synchroPeersToRemove_

        timer_.schedule(jobId_, TIME_LIMIT);
      } else {
        logger_.warn("Async messages context has not yet been initialized, ending the module");
        endJobModule();
      }
      return null;
    }

    public Void visit(AddedAsSynchroPeerMessage message) {
      if (synchroPeersToAdd_.contains(message.getSourceAddress())) {
        addedPeers_.add(message.getSourceAddress());
        if (addedPeers_.size() == synchroPeersToAdd_.size()) {
          addSynchroPeers();
        }
      } else {
        logger_.warn("Received " + message.getClass().getCanonicalName() + " from a peer that " +
            "should not be added to synchro peer set.");
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout in " + getClass().getCanonicalName() + ". " + addedPeers_.size() +
          " peers were successfully added.");
      addSynchroPeers();
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.warn("Unable to put synchro-peers to DHT.");
      endJobModule();
      return null;
    }

    public Void visit(OkDHTMessage message) {
      context_.addAllSynchroPeers(addedPeers_);
      endJobModule();
      return null;
    }

    private void addSynchroPeers() {
      InstanceMetadata metadata = new InstanceMetadata(appKey_);
      metadata.getSynchroGroup().addAll(addedPeers_);
      // TODO (pm) remove peers from peersToRemove_
      networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
    }
  }
}
