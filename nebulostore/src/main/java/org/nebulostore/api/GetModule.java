package org.nebulostore.api;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
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
 * @param <V> Returning type.
 * @author Bolek Kulbabinski
 */
public abstract class GetModule<V> extends ReturningJobModule<V> {
  private static Logger logger_ = Logger.getLogger(GetModule.class);
  private static final long REPLICA_WAIT_MILLIS = 5000L;

  protected NebuloAddress address_;
  protected CommAddress replicaAddress_;
  protected Timer timer_;
  protected EncryptionAPI encryption_;
  protected CommAddress myAddress_;

  protected String privateKeyPeerId_;
  protected Map<String, SecretKey> sessionKeys_ = new HashMap<String, SecretKey>();



  @Inject
  public void setDependencies(CommAddress myAddress,
                              Timer timer,
                              EncryptionAPI encryptionAPI,
                              @Named("PrivateKeyPeerId") String privateKeyPeerId) {
    myAddress_ = myAddress;
    timer_ = timer;
    encryption_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
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
  protected abstract class GetModuleVisitor extends MessageVisitor {
    protected STATE state_;
    protected SortedSet<CommAddress> replicationGroupSet_;

    public GetModuleVisitor() {
      state_ = STATE.INIT;
    }

    public void visit(JobInitMessage message) {
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
          replicationGroupSet_ = replicationGroup.getReplicatorSet();
          queryNextReplica();
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

    public void queryNextReplica() {
      if (replicationGroupSet_.size() == 0) {
        endWithError(new NebuloException("No replica responded in time."));
      } else {
        CommAddress replicator = replicationGroupSet_.first();
        replicationGroupSet_.remove(replicator);
        logger_.debug("Querying replica (" + replicator + ")");
        startSessionAgreement(replicator);
        timer_.schedule(jobId_, REPLICA_WAIT_MILLIS, STATE.REPLICA_FETCH.name());
      }
    }

    public void visit(TimeoutMessage message) {
      if (state_ == STATE.REPLICA_FETCH && state_.name().equals(message.getMessageContent())) {
        logger_.debug("Timeout - replica didn't respond in time. Trying another one.");
        queryNextReplica();
      }
    }

    public abstract Void visit(SendObjectMessage message);

    public void visit(ReplicatorErrorMessage message) {
      if (state_ == STATE.REPLICA_FETCH) {
        // TODO(bolek): ReplicatorErrorMessage should contain exception instead of string.
        endWithError(new NebuloException(message.getMessage()));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(InitSessionEndMessage message) {
      logger_.debug("Process " + message);
      sessionKeys_.put(message.getSessionId(), message.getSessionKey());
      networkQueue_.add(new GetObjectMessage(CryptoUtils.getRandomId().toString(),
          myAddress_, message.getPeerAddress(), address_.getObjectId(), jobId_,
          message.getSessionId()));
    }

    public void visit(InitSessionEndWithErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
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
