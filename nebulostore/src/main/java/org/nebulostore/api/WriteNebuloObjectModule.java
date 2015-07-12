package org.nebulostore.api;

import java.util.List;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.async.SendAsynchronousMessagesForPeerModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.UpdateNebuloObjectMessage;
import org.nebulostore.async.messages.UpdateSmallNebuloObjectMessage;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ObjectOutdatedMessage;

/**
 * @author Bolek Kulbabinski
 * @author szymonmatejczyk
 */

public class WriteNebuloObjectModule extends WriteModule implements ObjectWriter {
  private static Logger logger_ = Logger.getLogger(WriteNebuloObjectModule.class);
  /* small files below 1MB */
  private static final int SMALL_FILE_THRESHOLD = 1024 * 1024;

  /* number of confirmation messages required from replicas to return success */
  private static final int CONFIRMATIONS_REQUIRED = 2;

  private NebuloObject object_;
  private EncryptWrapper encryptWrapper_;

  private List<String> previousVersionSHAs_;

  private final ReplicaPlacementPreparator replicaPlacementPreparator_;
  private ReplicationGroup group_;

  @Inject
  public WriteNebuloObjectModule(EncryptionAPI encryption,
      ReplicaPlacementPreparator replicaPlacementPreparator) {
    super(encryption);
    replicaPlacementPreparator_ = replicaPlacementPreparator;
  }

  @Override
  protected WriteModuleVisitor createVisitor() {
    return new StateMachineVisitor();
  }

  @Override
  public void writeObject(NebuloObject objectToWrite, List<String> previousVersionSHAs,
      EncryptWrapper encryptWrapper) {
    object_ = objectToWrite;
    previousVersionSHAs_ = previousVersionSHAs;
    encryptWrapper_ = encryptWrapper;
    super.writeObject(CONFIRMATIONS_REQUIRED);
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  /**
   * Visitor class that acts as a state machine realizing the procedure of fetching the file.
   */
  protected class StateMachineVisitor extends WriteModuleVisitor {

    private boolean isProcessingDHTQuery_;
    private String commitVersion_;

    public void visit(JobInitMessage message) {
      if (state_ == STATE.INIT) {
        logger_.debug("Initializing...");
        // State 1 - Send groupId to DHT and wait for reply.
        isProcessingDHTQuery_ = true;
        jobId_ = message.getId();

        NebuloAddress address = object_.getAddress();
        logger_.debug("Adding GetDHT to network queue (" + address.getAppKey() + ", " +
            jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ValueDHTMessage message) {
      logger_.debug("Got ValueDHTMessage " + message.toString());
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {

        // Receive reply from DHT and iterate over logical path segments asking
        // for consecutive parts.
        isProcessingDHTQuery_ = false;

        // TODO(bolek): How to avoid casting here? Make ValueDHTMessage generic?
        // TODO(bolek): Merge this with similar part from GetNebuloFileModule?
        UserMetadata metadata = (UserMetadata) message.getValue().getValue();
        logger_.debug("Metadata: " + metadata);

        ContractList contractList = metadata.getContractList();
        logger_.debug("ContractList: " + contractList);
        group_ = contractList.getGroup(object_.getObjectId());

        logger_.debug("Group: " + group_);
        if (group_ == null) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          try {
            EncryptedObject encryptedObject = encryptWrapper_.encrypt(object_);
            commitVersion_ = CryptoUtils.sha(encryptedObject);
            boolean isSmallFile = encryptedObject.size() < SMALL_FILE_THRESHOLD;

            ReplicaPlacementData placementData =
                replicaPlacementPreparator_.prepareObject(encryptedObject.getEncryptedData(),
                    group_.getReplicators());
            sendStoreQueries(placementData.getReplicaPlacementMap(), previousVersionSHAs_,
                isSmallFile, commitVersion_, object_.getObjectId());
          } catch (CryptoException exception) {
            endWithError(new NebuloException("Unable to encrypt object.", exception));
          }
        }
      } else {
        incorrectState(state_.name(), message);
      }
    }

    public void visit(ErrorDHTMessage message) {
      if (state_ == STATE.INIT && isProcessingDHTQuery_) {
        logger_.debug("Received ErrorDHTMessage");
        endWithError(new NebuloException("Could not fetch metadata from DHT.",
            message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
    }

    @Override
    protected void respondToAnswer(TransactionAnswerInMessage message) {
      super.respondToAnswer(message);
      if (message.answer_ == TransactionAnswer.COMMIT) {
        // Peers that didn't response should get an AM.
        for (CommAddress deadReplicator : recipientsSet_) {
          // TODO (pm) This probably doesn't make sense without repetition code
          AsynchronousMessage asynchronousMessage =
              isSmallFile_ ? new UpdateSmallNebuloObjectMessage(object_) :
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
    }
  }
}
