package org.nebulostore.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.appcore.modules.TwoStepReturningJobModule;
import org.nebulostore.async.SendAsynchronousMessagesForPeerModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.UpdateNebuloObjectMessage;
import org.nebulostore.async.messages.UpdateSmallNebuloObjectMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.ObjectOutdatedMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;
import org.nebulostore.replicator.messages.UpdateRejectMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage;

/**
 * @author Bolek Kulbabinski
 * @author szymonmatejczyk
 */

public class WriteNebuloObjectModule extends TwoStepReturningJobModule<Void, Void,
    TransactionAnswer> implements ObjectWriter {
  private static Logger logger_ = Logger.getLogger(WriteNebuloObjectModule.class);
  /* small files below 1MB */
  private static final int SMALL_FILE_THRESHOLD = 1024 * 1024;

  /* number of confirmation messages required from replicas to return success */
  private static final int CONFIRMATIONS_REQUIRED = 2;

  private NebuloObject object_;

  private Set<String> previousVersionSHAs_;
  private final StateMachineVisitor visitor_ = new StateMachineVisitor();

  private String commitVersion_;
  private int nRecipients_;

  @Override
  public void writeObject(NebuloObject objectToWrite, Set<String> previousVersionSHAs) {
    object_ = objectToWrite;
    previousVersionSHAs_ = previousVersionSHAs;
    runThroughDispatcher();
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  /**
   * States of the state machine.
   */
  private enum STATE { INIT, DHT_QUERY, REPLICA_UPDATE, RETURNED_WAITING_FOR_REST, DONE };

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected class StateMachineVisitor extends MessageVisitor<Void> {
    private STATE state_;
    /* Recipients we are waiting answer from. */
    private final Set<CommAddress> recipientsSet_ = new HashSet<>();

    /* Repicators that rejected transaction, when it has been already commited. */
    private final Set<CommAddress> rejectingOrWithholdingReplicators_ = new HashSet<>();

    /* CommAddress -> JobId of peers waiting for transaction result */
    private final Map<CommAddress, String> waitingForTransactionResult_ = new HashMap<>();

    private boolean isSmallFile_;
    private int confirmations_;

    public StateMachineVisitor() {
      state_ = STATE.INIT;
    }

    public Void visit(JobInitMessage message) {
      if (state_ == STATE.INIT) {
        logger_.debug("Initializing...");
        // State 1 - Send groupId to DHT and wait for reply.
        state_ = STATE.DHT_QUERY;
        jobId_ = message.getId();

        NebuloAddress address = object_.getAddress();
        logger_.debug("Adding GetDHT to network queue (" + address.getAppKey() + ", " +
            jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      logger_.debug("Got ValueDHTMessage " + message.toString());
      if (state_ == STATE.DHT_QUERY) {

        // State 2 - Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        state_ = STATE.REPLICA_UPDATE;
        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        // TODO(bolek): Merge this with similar part from GetNebuloFileModule?
        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Metadata: " + metadata);

        ContractList contractList = metadata.getContractList();
        logger_.debug("ContractList: " + contractList);
        ReplicationGroup group = contractList.getGroup(object_.getObjectId());
        logger_.debug("Group: " + group);
        if (group == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          EncryptedObject encryptedObject = null;
          try {
            encryptedObject = CryptoUtils.encryptObject(object_);
            commitVersion_ = CryptoUtils.sha(encryptedObject);
            isSmallFile_ = encryptedObject.size() < SMALL_FILE_THRESHOLD;

            for (CommAddress replicator : group) {
              String remoteJobId = CryptoUtils.getRandomId().toString();
              waitingForTransactionResult_.put(replicator, remoteJobId);
              networkQueue_.add(new QueryToStoreObjectMessage(remoteJobId, replicator,
                  object_.getObjectId(), encryptedObject, previousVersionSHAs_, getJobId()));
              logger_.debug("added recipient: " + replicator);
            }
            recipientsSet_.addAll(group.getReplicatorSet());
            nRecipients_ += group.getSize();
          } catch (CryptoException exception) {
            endWithError(new NebuloException("Unable to encrypt object.", exception));
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
      logger_.debug("received confirmation");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        confirmations_++;
        recipientsSet_.remove(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(UpdateRejectMessage message) {
      logger_.debug("received updateRejectMessage");
      switch (state_) {
        case REPLICA_UPDATE:
          recipientsSet_.remove(message.getSourceAddress());
          sendTransactionAnswer(TransactionAnswer.ABORT);
          endWithError(new NebuloException("Update failed due to inconsistent state."));
          break;
        case RETURNED_WAITING_FOR_REST:
          recipientsSet_.remove(message.getSourceAddress());
          waitingForTransactionResult_.remove(message.getDestinationAddress());
          rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
          logger_.warn("Inconsitent state among replicas.");
          break;
        default:
          incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(UpdateWithholdMessage message) {
      logger_.debug("reject UpdateWithholdMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        recipientsSet_.remove(message.getSourceAddress());
        waitingForTransactionResult_.remove(message.getDestinationAddress());
        rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      logger_.debug("received ErrorCommMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        waitingForTransactionResult_.remove(message.getMessage().getDestinationAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    private void tryReturnSemiResult() {
      logger_.debug("trying to return semi result");
      if (recipientsSet_.isEmpty() &&
          confirmations_ < Math.min(CONFIRMATIONS_REQUIRED, nRecipients_)) {
        sendTransactionAnswer(TransactionAnswer.ABORT);
        endWithError(new NebuloException("Not enough replicas responding to update file (" +
          confirmations_ + "/" + Math.min(CONFIRMATIONS_REQUIRED, nRecipients_) + ")."));
      } else {
        if (!isSmallFile_) {
          /* big file - requires only CONFIRMATIONS_REQUIERED ConfirmationMessages,
           * returns from write and updates other replicas in background */
          if (confirmations_ >= CONFIRMATIONS_REQUIRED && state_ == STATE.REPLICA_UPDATE) {
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
            state_ = STATE.RETURNED_WAITING_FOR_REST;
          } else if (recipientsSet_.isEmpty()) {
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
          }
        } else {
          if (recipientsSet_.isEmpty()) {
            logger_.debug("Query phase completed, waiting for result.");
            returnSemiResult(null);
          }
        }
      }
    }

    public Void visit(TransactionAnswerInMessage message) {
      logger_.debug("received TransactionResult from parent");
      sendTransactionAnswer(message.answer_);
      if (message.answer_ == TransactionAnswer.COMMIT) {
        // Peers that didn't response should get an AM.
        for (CommAddress deadReplicator : recipientsSet_) {
          AsynchronousMessage asynchronousMessage = isSmallFile_ ?
              new UpdateSmallNebuloObjectMessage(object_) :
                new UpdateNebuloObjectMessage(object_.getAddress(), null);

          new SendAsynchronousMessagesForPeerModule(deadReplicator, asynchronousMessage, outQueue_);
        }
        // Peers that rejected or withheld transaction should get notification, that their
        // version is outdated.
        for (CommAddress rejecting : rejectingOrWithholdingReplicators_) {
          networkQueue_.add(new ObjectOutdatedMessage(rejecting, object_.getAddress()));
        }

        // TODO(szm): don't like updating version here
        object_.newVersionCommitted(commitVersion_);
      }
      endWithSuccess(null);
      return null;
    }

    private void sendTransactionAnswer(TransactionAnswer answer) {
      logger_.debug("sending transaction answer");
      for (Map.Entry<CommAddress, String> entry : waitingForTransactionResult_.entrySet()) {
        networkQueue_.add(new TransactionResultMessage(entry.getValue(), entry.getKey(), answer));
      }
    }

    // TODO(bolek): Maybe move it to a new superclass StateMachine?
    private void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }

  /**
   * Just for readability - inner and private message in WriteNebuloObject.
   * @author szymonmatejczyk
   */
  public static class TransactionAnswerInMessage extends Message {
    private static final long serialVersionUID = 3862738899180300188L;

    TransactionAnswer answer_;

    public TransactionAnswerInMessage(TransactionAnswer answer) {
      answer_ = answer;
    }

    @Override
    public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
      return visitor.visit(this);
    }
  }

  @Override
  protected void performSecondPhase(TransactionAnswer answer) {
    logger_.debug("Performing second phase");
    inQueue_.add(new TransactionAnswerInMessage(answer));
  }
}
