package org.nebulostore.api;

import java.util.SortedSet;
import java.util.TreeSet;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Job module that fetches V from NebuloStore.
 * Dependencies: object address. Optional: replica comm-address.
 * @param <V> Returning type.
 * @author Bolek Kulbabinski
 */
public abstract class GetModule<V> extends ReturningJobModule<V> {
  private static Logger logger_ = Logger.getLogger(GetModule.class);
  private static final long REPLICA_WAIT_MILLIS = 5000L;

  protected NebuloAddress address_;
  protected CommAddress replicaAddress_;
  protected Timer timer_;

  @Inject
  public void setTimer(Timer timer) {
    timer_ = timer;
  }

  public void fetchObject(NebuloAddress address, CommAddress replicaAddress) {
    address_ = checkNotNull(address);
    replicaAddress_ = replicaAddress;
    runThroughDispatcher();
  }

  /**
   * States of the state machine.
   */
  protected enum STATE { INIT, DHT_QUERY, REPLICA_FETCH, FILE_RECEIVED };

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected abstract class GetModuleVisitor extends MessageVisitor<Void> {
    protected STATE state_;
    protected SortedSet<CommAddress> replicationGroupSet_;

    public GetModuleVisitor() {
      state_ = STATE.INIT;
    }

    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Retrieving file " + address_);

      if (state_ == STATE.INIT) {
        if (replicaAddress_ == null) {
          // State 1 - Send groupId to DHT and wait for reply.
          state_ = STATE.DHT_QUERY;
          logger_.debug("Adding GetDHT to network queue (" + address_.getAppKey() + ", " + jobId_ +
              ").");
          networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address_.getAppKey().getKey())));
        } else {
          state_ = STATE.REPLICA_FETCH;
          replicationGroupSet_ = new TreeSet<CommAddress>();
          replicationGroupSet_.add(replicaAddress_);
          queryNextReplica();
        }
      } else {
        incorrectState(state_.name(), message);
      }

      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (state_ == STATE.DHT_QUERY) {

        // State 2 - Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        state_ = STATE.REPLICA_FETCH;
        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Received ValueDHTMessage: " + metadata.toString());
        ContractList contractList = metadata.getContractList();
        ReplicationGroup replicationGroup = contractList.getGroup(address_.getObjectId());
        if (replicationGroup == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          replicationGroupSet_ = replicationGroup.getReplicatorSet();
          queryNextReplica();
        }
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      if (state_ == STATE.DHT_QUERY) {
        logger_.debug("Received ErrorDHTMessage");
        endWithError(new NebuloException("Could not fetch metadata from DHT.",
            message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public void queryNextReplica() {
      if (replicationGroupSet_.size() == 0) {
        endWithError(new NebuloException("No replica responded in time."));
      } else {
        CommAddress replicator = replicationGroupSet_.first();
        replicationGroupSet_.remove(replicator);
        logger_.debug("Querying replica (" + replicator + ")");
        networkQueue_.add(new GetObjectMessage(CryptoUtils.getRandomId().toString(),
            replicator, address_.getObjectId(), jobId_));
        timer_.schedule(jobId_, REPLICA_WAIT_MILLIS, STATE.REPLICA_FETCH.name());
      }
    }

    public Void visit(TimeoutMessage message) {
      if (state_ == STATE.REPLICA_FETCH && state_.name().equals(message.getMessageContent())) {
        logger_.debug("Timeout - replica didn't respond in time. Trying another one.");
        queryNextReplica();
      }
      return null;
    }

    public abstract Void visit(SendObjectMessage message);

    public Void visit(ReplicatorErrorMessage message) {
      if (state_ == STATE.REPLICA_FETCH) {
        // TODO(bolek): ReplicatorErrorMessage should contain exception instead of string.
        endWithError(new NebuloException(message.getMessage()));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    protected void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }
  }

}
