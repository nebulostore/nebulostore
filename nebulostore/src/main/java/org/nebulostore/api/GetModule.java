package org.nebulostore.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.InitSessionNegotiatorModule;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
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
 *
 * @param <V> Returning type.
 * @author Bolek Kulbabinski
 */
public abstract class GetModule<V> extends ReturningJobModule<V> {
  private static Logger logger_ = Logger.getLogger(GetModule.class);
  private static final long REPLICA_WAIT_MILLIS = 5000L;

  protected NebuloAddress address_;
  protected Provider<Timer> timerProvider_;
  protected EncryptionAPI encryption_;
  protected CommAddress myAddress_;
  protected ObjectRecreator recreator_;

  protected final Set<CommAddress> waitingForReplicators_ = new HashSet<>();

  protected String privateKeyPeerId_;
  protected Map<String, SecretKey> sessionKeys_ = new HashMap<String, SecretKey>();

  @Inject
  public void setDependencies(CommAddress myAddress, Provider<Timer> timerProvider,
      EncryptionAPI encryptionAPI, @Named("PrivateKeyPeerId") String privateKeyPeerId,
      ObjectRecreator recreator) {
    myAddress_ = myAddress;
    timerProvider_ = timerProvider;
    encryption_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
    recreator_ = recreator;
  }

  public void fetchObject(NebuloAddress address) {
    address_ = checkNotNull(address);
    runThroughDispatcher();
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
  protected abstract class GetModuleVisitor extends MessageVisitor {
    protected STATE state_;
    protected List<CommAddress> replicationGroupList_;

    public GetModuleVisitor() {
      state_ = STATE.INIT;
    }

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
        Metadata metadata = (Metadata) message.getValue().getValue();
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

    public void queryNextReplicas() {
      if (replicationGroupList_.isEmpty()) {
        endWithError(new NebuloException("Not enough replicas responded in time."));
      } else {
        Iterator<CommAddress> iterator = replicationGroupList_.iterator();
        while (iterator.hasNext()) {
          CommAddress replicator = iterator.next();
          if (!waitingForReplicators_.contains(replicator)) {
            logger_.debug("Querying replica (" + replicator + ")");
            waitingForReplicators_.add(replicator);
            iterator.remove();
            startSessionAgreement(replicator);
          }
        }
        timerProvider_.get().schedule(jobId_, REPLICA_WAIT_MILLIS, STATE.REPLICA_FETCH.name());
      }
    }

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

    public abstract void visit(SendObjectMessage message);

    public void visit(ReplicatorErrorMessage message) {
      if (state_ == STATE.REPLICA_FETCH) {
        failReplicator(message.getSourceAddress());
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(InitSessionEndMessage message) {
      logger_.debug("Process " + message);
      sessionKeys_.put(message.getSessionId(), message.getSessionKey());
      networkQueue_.add(new GetObjectMessage(CryptoUtils.getRandomId().toString(), myAddress_,
          message.getPeerAddress(), address_.getObjectId(), jobId_, message.getSessionId()));
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
      failReplicator(message.getPeerAddress());
    }

    protected EncryptedObject tryRecreateFullObject(SendObjectMessage message)
        throws NebuloException {
      if (waitingForReplicators_.remove(message.getSourceAddress())) {
        if (state_ == STATE.REPLICA_FETCH) {
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

    protected void failReplicator(CommAddress replicator) {
      recreator_.removeReplicator(replicator);
      waitingForReplicators_.remove(replicator);
      replicationGroupList_ = recreator_.calcReplicatorsToAsk();
      queryNextReplicas();
    }

    protected void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }

    protected EncryptedObject decryptWithSessionKey(EncryptedObject cipher, String sessionId)
        throws CryptoException {
      return (EncryptedObject) encryption_.decryptWithSessionKey(cipher,
          sessionKeys_.remove(sessionId));
    }
  }

  private void startSessionAgreement(CommAddress replicator) {
    InitSessionNegotiatorModule initSessionNegotiatorModule =
        new InitSessionNegotiatorModule(replicator, getJobId(), null);
    outQueue_.add(new JobInitMessage(initSessionNegotiatorModule));
  }

}
