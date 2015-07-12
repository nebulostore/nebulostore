package org.nebulostore.api;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Job module that fetches full object of type V from NebuloStore. Dependencies: object address.
 * Optional: replica comm-address.
 *
 * @param <V>
 *          Returning type.
 * @author Bolek Kulbabinski
 */
public abstract class GetFullObjectModule<V> extends GetModule<V> {
  private static Logger logger_ = Logger.getLogger(GetFullObjectModule.class);

  protected ObjectRecreator recreator_;

  @Inject
  public void setDependencies(Provider<Timer> timerProvider,
      EncryptionAPI encryptionAPI, ObjectRecreator recreator) {
    super.setDependencies(timerProvider, encryptionAPI);
    recreator_ = recreator;
  }

  /**
   * States of the state machine.
   */
  protected enum STATE {
    INIT, DHT_QUERY, REPLICA_FETCH, FILE_RECEIVED
  };

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected abstract class GetFullObjectModuleVisitor extends GetModuleVisitor {

    @Override
    public void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Retrieving file " + address_);

      if (state_ == STATE.INIT) {
        // State 1 - Send groupId to DHT and wait for reply.
        state_ = STATE.DHT_QUERY;
        logger_.debug("Adding GetDHT to network queue (" + address_.getAppKey() + ", " + jobId_ +
            ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address_.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ValueDHTMessage message) {
      if (state_ == STATE.DHT_QUERY) {

        // State 2 - Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        state_ = STATE.REPLICA_FETCH;
        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        UserMetadata metadata = (UserMetadata) message.getValue().getValue();
        logger_.debug("Received ValueDHTMessage: " + metadata.toString());
        ContractList contractList = metadata.getContractList();
        ReplicationGroup replicationGroup = contractList.getGroup(address_.getObjectId());
        if (replicationGroup == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          recreator_.setReplicators(replicationGroup.getReplicators());
          replicationGroupList_ = recreator_.calcReplicatorsToAsk();

          queryNextReplicas();
        }
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (state_ == STATE.DHT_QUERY) {
        logger_.debug("Received ErrorDHTMessage");
        endWithError(new NebuloException("Could not fetch metadata from DHT.",
            message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    @Override
    public void visit(TimeoutMessage message) {
      if (state_ == STATE.REPLICA_FETCH && state_.name().equals(message.getMessageContent())) {
        logger_.debug("Timeout - replica didn't respond in time. Trying another one.");
        for (CommAddress replicator : waitingForReplicators_) {
          recreator_.removeReplicator(replicator);
        }
        replicationGroupList_ = recreator_.calcReplicatorsToAsk();
        queryNextReplicas();
      }
    }

    protected EncryptedObject tryRecreateFullObject(SendObjectMessage message)
        throws NebuloException {
      if (waitingForReplicators_.remove(message.getSourceAddress())) {
        if (state_ == STATE.REPLICA_FETCH) {
          boolean versionUpdated = false;

          try {
            if (checkVersion(message)) {
              // Version was updated, starting again
              recreator_.clearReceivedFragments();
              versionUpdated = true;
            }
          } catch (NebuloException e) {
            logger_.warn(e);
            return null;
          }

          EncryptedObject object =
              decryptWithSessionKey(message.getEncryptedEntity(), message.getSessionId());

          logger_.debug("Got next file fragment");

          EncryptedObject fullObject = null;
          if (recreator_.addNextFragment(object, message.getSourceAddress())) {
            logger_.debug("Got object - returning");

            fullObject = recreator_.recreateObject();
            // State 3 - Finally got the file, return it;
            state_ = STATE.FILE_RECEIVED;
          }

          if (versionUpdated && fullObject == null) {
            replicationGroupList_ = recreator_.calcReplicatorsToAsk();
            queryNextReplicas();
          }

          return fullObject;
        } else {
          logger_.warn("SendObjectMessage received in state " + state_);
          return null;
        }
      } else {
        logger_.warn("Received an object fragment from an unexpected replicator.");
        return null;
      }
    }

    @Override
    protected void failReplicator(CommAddress replicator) {
      recreator_.removeReplicator(replicator);
      waitingForReplicators_.remove(replicator);
      replicationGroupList_ = recreator_.calcReplicatorsToAsk();
      queryNextReplicas();
    }
  }
}
