package org.nebulostore.api;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.ObjectDeleter;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;

/**
 * @author Bolek Kulbabinski
 */
public class DeleteNebuloObjectModule extends ReturningJobModule<Void> implements ObjectDeleter {
  private static Logger logger_ = Logger.getLogger(DeleteNebuloObjectModule.class);

  private NebuloAddress address_;
  private final StateMachineVisitor visitor_ = new StateMachineVisitor();

  @Override
  public void deleteObject(NebuloAddress address) {
    address_ = address;
    runThroughDispatcher();
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  /**
   * States of the state machine.
   */
  private enum STATE { INIT, DHT_QUERY, REPLICA_UPDATE, DONE };

  /**
   * Visitor class that acts as a state machine realizing the procedure of deleting the file.
   */
  protected class StateMachineVisitor extends MessageVisitor<Void> {
    private final Set<CommAddress> recipientsSet_;
    private STATE state_;

    public StateMachineVisitor() {
      recipientsSet_ = new HashSet<CommAddress>();
      state_ = STATE.INIT;
    }

    public Void visit(JobInitMessage message) {
      if (state_ == STATE.INIT) {
        // State 1 - Send groupId to DHT and wait for reply.
        state_ = STATE.DHT_QUERY;
        jobId_ = message.getId();

        logger_.debug("Adding GetDHT to network queue (" + address_.getAppKey() + ", " +
            jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address_.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      logger_.debug("Got ValueDHTMessage " + message.toString());
      if (state_ == STATE.DHT_QUERY) {
        state_ = STATE.REPLICA_UPDATE;
        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        // TODO(bolek): Merge this with similar part from GetNebuloFileModule?
        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Metadata: " + metadata);

        ContractList contractList = metadata.getContractList();
        logger_.debug("ContractList: " + contractList);
        ReplicationGroup group = contractList.getGroup(address_.getObjectId());
        logger_.debug("Replication group: " + group);
        if (group == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          for (CommAddress replicator : group) {
            String remoteJobId = CryptoUtils.getRandomId().toString();
            networkQueue_.add(new DeleteObjectMessage(remoteJobId, replicator,
                address_.getObjectId(), getJobId()));
            recipientsSet_.add(replicator);
          }
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

    public Void visit(ConfirmationMessage message) {
      if (state_ == STATE.REPLICA_UPDATE) {
        logger_.debug("Confirmation message, removing: " + message.getSourceAddress());
        recipientsSet_.remove(message.getSourceAddress());
        if (recipientsSet_.isEmpty()) {
          logger_.debug("All recipients have replied. Finishing.");
          // All recipients replied. From now we can ignore DeleteTimeoutMessage and finish module.
          // TODO(bolek): delete timer message?
          state_ = STATE.DONE;
          endWithSuccess(null);
        }
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      incorrectState(state_.name(), message);
      // TODO(bolek): Can we safely ignore it here and just wait for timeout and async messages?
      return null;
    }

    // TODO(bolek): Maybe move it to a new superclass StateMachine?
    private void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }
  }

  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
