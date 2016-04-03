package org.nebulostore.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.TwoStepReturningJobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.SessionObject;
import org.nebulostore.crypto.session.SessionObjectMap;
import org.nebulostore.crypto.session.message.DHFinishMessage;
import org.nebulostore.crypto.session.message.DHLocalErrorMessage;
import org.nebulostore.replicator.core.StoreData;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;
import org.nebulostore.replicator.messages.UpdateRejectMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage;

/**
 * @author Bolek Kulbabinski
 * @author szymonmatejczyk
 */

public abstract class WriteModule extends TwoStepReturningJobModule<Void, Void, TransactionAnswer> {
  private static Logger logger_ = Logger.getLogger(WriteNebuloObjectModule.class);

  private final WriteModuleVisitor visitor_;

  private int nRecipients_;

  /* number of confirmation messages required from replicas to return success */
  private int confirmationsRequired_;

  private Map<CommAddress, EncryptedObject> objectsMap_;
  private ObjectId objectId_;

  protected final EncryptionAPI encryption_;
  protected SessionObjectMap sessionObjectMap_;

  /**
   * States of the state machine.
   */
  protected enum STATE {
    INIT, REPLICA_UPDATE, RETURNED_WAITING_FOR_REST, DONE
  };

  protected WriteModule(EncryptionAPI encryption) {
    encryption_ = encryption;
    visitor_ = createVisitor();
  }

  protected abstract WriteModuleVisitor createVisitor();

  protected void writeObject(int confirmationsRequired) {
    confirmationsRequired_ = confirmationsRequired;
    runThroughDispatcher();
  }

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected abstract class WriteModuleVisitor extends MessageVisitor {
    protected STATE state_;
    /* Recipients we are waiting answer from. */
    protected final Set<CommAddress> recipientsSet_ = new HashSet<>();

    /* Repicators that rejected transaction, when it has been already commited. */
    protected final Set<CommAddress> rejectingOrWithholdingReplicators_ = new HashSet<>();

    /* CommAddress -> JobId of peers waiting for transaction result */
    private final Map<CommAddress, String> waitingForTransactionResult_ = new HashMap<>();

    protected boolean isSmallFile_;
    private int confirmations_;

    public WriteModuleVisitor() {
      state_ = STATE.INIT;
    }

    /**
     * Sends requests to store given object fragments.
     *
     * @param objectsMap
     * @param previousVersionSHAs
     * @param isSmallFile
     * @param commitVersion
     *          Version of this commit. If version was not changed, it should be equal to null
     * @param objectId
     * @throws CryptoException
     */
    protected void sendStoreQueries(Map<CommAddress, EncryptedObject> objectsMap,
        List<String> previousVersionSHAs, boolean isSmallFile, String commitVersion,
        ObjectId objectId) throws CryptoException {
      state_ = STATE.REPLICA_UPDATE;
      isSmallFile_ = isSmallFile;
      objectsMap_ = objectsMap;
      objectId_ = objectId;
      for (Entry<CommAddress, EncryptedObject> placementEntry : objectsMap_.entrySet()) {
        String remoteJobId = CryptoUtils.getRandomId().toString();
        SessionObject sessionObject = sessionObjectMap_.get(placementEntry.getKey());
        waitingForTransactionResult_.put(placementEntry.getKey(), remoteJobId);
        EncryptedObject data = encryption_.encryptWithSessionKey(placementEntry.getValue(),
            sessionObject.getSessionKey());
        networkQueue_.add(new QueryToStoreObjectMessage(remoteJobId,
            placementEntry.getKey(), objectId_, data,
            previousVersionSHAs, getJobId(),
            commitVersion, sessionObject.getSessionId()));
        logger_.debug("added recipient: " + placementEntry.getKey());
      }
      recipientsSet_.addAll(objectsMap_.keySet());
      nRecipients_ += objectsMap_.keySet().size();
    }

    public void visit(ConfirmationMessage message) {
      logger_.debug("received confirmation");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        confirmations_++;
        recipientsSet_.remove(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(UpdateRejectMessage message) {
      logger_.debug("received updateRejectMessage");
      switch (state_) {
        case REPLICA_UPDATE :
          recipientsSet_.remove(message.getSourceAddress());
          sendTransactionAnswer(TransactionAnswer.ABORT);
          endWithError(new NebuloException("Update failed due to inconsistent state."));
          break;
        case RETURNED_WAITING_FOR_REST :
          recipientsSet_.remove(message.getSourceAddress());
          waitingForTransactionResult_.remove(message.getDestinationAddress());
          rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
          logger_.warn("Inconsitent state among replicas.");
          break;
        default :
          incorrectState(state_.name(), message);
      }
    }

    public void visit(UpdateWithholdMessage message) {
      logger_.debug("reject UpdateWithholdMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        recipientsSet_.remove(message.getSourceAddress());
        waitingForTransactionResult_.remove(message.getDestinationAddress());
        rejectingOrWithholdingReplicators_.add(message.getSourceAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorCommMessage message) {
      logger_.debug("received ErrorCommMessage");
      if (state_ == STATE.REPLICA_UPDATE || state_ == STATE.RETURNED_WAITING_FOR_REST) {
        waitingForTransactionResult_.remove(message.getMessage().getDestinationAddress());
        recipientsSet_.remove(message.getMessage().getDestinationAddress());
        tryReturnSemiResult();
      } else {
        incorrectState(state_.name(), message);
      }
    }

    private void tryReturnSemiResult() {
      logger_.debug("trying to return semi result");
      if (recipientsSet_.isEmpty() &&
          confirmations_ < Math.min(confirmationsRequired_, nRecipients_)) {
        sendTransactionAnswer(TransactionAnswer.ABORT);
        endWithError(new NebuloException("Not enough replicas responding to update file (" +
            confirmations_ + "/" + Math.min(confirmationsRequired_, nRecipients_) + ")."));
      } else {
        if (!isSmallFile_) {
          /*
           * big file - requires only CONFIRMATIONS_REQUIRED ConfirmationMessages, returns from
           * write and updates other replicas in background
           */
          if (confirmations_ >= confirmationsRequired_ && state_ == STATE.REPLICA_UPDATE) {
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

    public void visit(TransactionAnswerInMessage message) {
      logger_.debug("received TransactionResult from parent");
      respondToAnswer(message);
      endWithSuccess(null);
    }

    public void visit(DHFinishMessage message) {
      logger_.debug("Process " + message);
      StoreData storeData = (StoreData) message.getData();
      try {
        EncryptedObject data = encryption_.encryptWithSessionKey(
            storeData.getData(), message.getSessionKey());
        networkQueue_.add(new QueryToStoreObjectMessage(storeData.getRemoteJobId(),
            message.getPeerAddress(), storeData.getObjectId(), data,
            storeData.getPreviousVersionSHAs(), getJobId(),
            storeData.getNewVersionSHA(), message.getSessionId()));
      } catch (CryptoException e) {
        endWithError(e);
      }
    }

    public void visit(DHLocalErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
    }

    protected void respondToAnswer(TransactionAnswerInMessage message) {
      sendTransactionAnswer(message.answer_);
    }

    protected void sendTransactionAnswer(TransactionAnswer answer) {
      logger_.debug("sending transaction answer");
      for (Map.Entry<CommAddress, String> entry : waitingForTransactionResult_.entrySet()) {
        networkQueue_.add(new TransactionResultMessage(entry.getValue(), entry.getKey(), answer));
      }
    }

    // TODO(bolek): Maybe move it to a new superclass StateMachine?
    protected void incorrectState(String stateName, Message message) {
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
   *
   * @author szymonmatejczyk
   */
  public static class TransactionAnswerInMessage extends Message {
    private static final long serialVersionUID = 3862738899180300188L;

    TransactionAnswer answer_;

    public TransactionAnswerInMessage(TransactionAnswer answer) {
      answer_ = answer;
    }
  }

  @Override
  protected void performSecondPhase(TransactionAnswer answer) {
    logger_.debug("Performing second phase");
    inQueue_.add(new TransactionAnswerInMessage(answer));
  }
}
