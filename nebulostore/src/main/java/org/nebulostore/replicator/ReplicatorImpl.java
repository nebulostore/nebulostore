package org.nebulostore.replicator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.log4j.Logger;
import org.nebulostore.api.GetEncryptedObjectModule;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.DeleteObjectException;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.ObjectOutdatedMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;
import org.nebulostore.replicator.messages.UpdateRejectMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage;
import org.nebulostore.replicator.messages.UpdateWithholdMessage.Reason;
import org.nebulostore.utils.LockMap;
import org.nebulostore.utils.Pair;

/**
 * Replicator - disk interface.
 *
 * @author szymonmatejczyk
 * @author Bolek Kulbabinski
 */
public class ReplicatorImpl extends Replicator {
  private static Logger logger_ = Logger.getLogger(ReplicatorImpl.class);

  private static final int UPDATE_TIMEOUT_SEC = 10;
  private static final int LOCK_TIMEOUT_SEC = 10;
  private static final int GET_OBJECT_TIMEOUT_SEC = 10;

  private static final String METADATA_SUFFIX = ".meta";

  private static LockMap lockMap_ = new LockMap();

  private final KeyValueStore<byte[]> store_;
  private final MessageVisitor<Void> visitor_ = new ReplicatorVisitor();

  @Inject
  public ReplicatorImpl(@Named("ReplicatorStore") KeyValueStore<byte[]> store) {
    store_ = store;
  }

  /**
   * Result of queryToStore.
   */
  private enum QueryToStoreResult { OK, OBJECT_OUT_OF_DATE, INVALID_VERSION, SAVE_FAILED, TIMEOUT }

  /**
   * Visitor to handle different message types.
   * @author szymonmatejczyk
   */
  protected class ReplicatorVisitor extends MessageVisitor<Void> {
    private QueryToStoreObjectMessage storeWaitingForCommit_;

    public Void visit(QueryToStoreObjectMessage message) throws NebuloException {
      logger_.debug("StoreObjectMessage received");

      QueryToStoreResult result = queryToUpdateObject(message.getObjectId(),
          message.getEncryptedEntity(), message.getPreviousVersionSHAs(), message.getId());
      logger_.debug("queryToUpdateObject returned " + result.name());
      switch (result) {
        case OK:
          networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
              message.getSourceAddress()));
          storeWaitingForCommit_ = message;
          try {
            TransactionResultMessage m = (TransactionResultMessage) inQueue_.poll(LOCK_TIMEOUT_SEC,
                TimeUnit.SECONDS);
            if (m == null) {
              abortUpdateObject(message.getObjectId(), message.getId());
              logger_.warn("Transaction aborted - timeout.");
            } else {
              processMessage(m);
            }
          } catch (InterruptedException exception) {
            abortUpdateObject(message.getObjectId(), message.getId());
            throw new NebuloException("Timeout while handling QueryToStoreObjectMessage",
                exception);
          } catch (ClassCastException exception) {
            abortUpdateObject(message.getObjectId(), message.getId());
            throw new NebuloException("Wrong message type received.", exception);
          }
          endJobModule();
          break;
        case OBJECT_OUT_OF_DATE:
          networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
              message.getSourceAddress(), Reason.OBJECT_OUT_OF_DATE));
          endJobModule();
          break;
        case INVALID_VERSION:
          networkQueue_.add(new UpdateRejectMessage(message.getSourceJobId(),
              message.getSourceAddress()));
          endJobModule();
          break;
        case SAVE_FAILED:
          networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
              message.getSourceAddress(), Reason.SAVE_FAILURE));
          break;
        case TIMEOUT:
          networkQueue_.add(new UpdateWithholdMessage(message.getSourceJobId(),
              message.getSourceAddress(), Reason.TIMEOUT));
          endJobModule();
          break;
        default:
          break;
      }
      return null;
    }

    public Void visit(TransactionResultMessage message) {
      logger_.debug("TransactionResultMessage received: " + message.getResult());
      if (storeWaitingForCommit_ == null) {
        //TODO(szm): ignore late abort transaction messages send by timer.
        logger_.warn("Unexpected commit message received.");
        endJobModule();
        return null;
      }
      if (message.getResult() == TransactionAnswer.COMMIT) {
        commitUpdateObject(storeWaitingForCommit_.getObjectId(),
                           storeWaitingForCommit_.getPreviousVersionSHAs(),
                           CryptoUtils.sha(storeWaitingForCommit_.getEncryptedEntity()),
                           message.getId());
      } else {
        abortUpdateObject(storeWaitingForCommit_.getObjectId(), message.getId());
      }
      endJobModule();
      return null;
    }

    public Void visit(GetObjectMessage message) {
      logger_.debug("GetObjectMessage with objectID = " + message.getObjectId());
      EncryptedObject enc = getObject(message.getObjectId());
      Set<String> versions;
      try {
        versions = getPreviousVersions(message.getObjectId());
      } catch (IOException e) {
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to retrieve object.");
        return null;
      }

      if (enc == null) {
        logger_.debug("Could not retrieve given object. Dying with error.");
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to retrieve object.");
      } else {
        networkQueue_.add(new SendObjectMessage(message.getSourceJobId(),
            message.getSourceAddress(), enc, versions));
        endJobModule();
      }
      return null;
    }

    public Void visit(DeleteObjectMessage message) {
      try {
        deleteObject(message.getObjectId());
        networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
            message.getSourceAddress()));
      } catch (DeleteObjectException exception) {
        logger_.warn(exception.toString());
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), exception.getMessage());
      }
      endJobModule();
      return null;
    }

    public Void visit(ObjectOutdatedMessage message) {
      try {
        GetEncryptedObjectModule getModule = new GetEncryptedObjectModule(message.getAddress(),
            outQueue_);
        Pair<EncryptedObject, Set<String>> res = getModule.getResult(GET_OBJECT_TIMEOUT_SEC);
        EncryptedObject encryptedObject = res.getFirst();
        try {
          deleteObject(message.getAddress().getObjectId());
        } catch (DeleteObjectException exception) {
          logger_.warn("Error deleting file.");
        }

        QueryToStoreResult query = queryToUpdateObject(message.getAddress().getObjectId(),
            encryptedObject, res.getSecond(), message.getId());
        if (query == QueryToStoreResult.OK || query == QueryToStoreResult.OBJECT_OUT_OF_DATE ||
            query == QueryToStoreResult.INVALID_VERSION) {
          commitUpdateObject(message.getAddress().getObjectId(), res.getSecond(),
              CryptoUtils.sha(encryptedObject), message.getId());
        } else {
          throw new NebuloException("Unable to fetch new version of file.");
        }
      } catch (NebuloException exception) {
        logger_.warn(exception);
      }
      return null;
    }

    private void dieWithError(String jobId, CommAddress sourceAddress,
        CommAddress destinationAddress, String errorMessage) {
      networkQueue_.add(new ReplicatorErrorMessage(jobId, destinationAddress, errorMessage));
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Begins transaction: tries to store object to temporal location.
   */
  public QueryToStoreResult queryToUpdateObject(ObjectId objectId,
      EncryptedObject encryptedObject, Set<String> previousVersions, String transactionToken) {
    logger_.debug("Checking store consistency");
    try {
      if (!lockMap_.tryLock(objectId.toString(), UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId + " lock timeout in queryToUpdateObject().");
        return QueryToStoreResult.TIMEOUT;
      }
    } catch (InterruptedException exception) {
      logger_.warn("Interrupted while waiting for object lock in queryToUpdateObject()");
      return QueryToStoreResult.TIMEOUT;
    }

    try {
      byte[] metaData = store_.get(objectId.toString() + METADATA_SUFFIX);
      if (metaData != null) {
        Set<String> localPreviousVersions;
        localPreviousVersions = getPreviousVersions(objectId);

        // checking whether remote file is up to date (update is not concurrent)
        if (!previousVersions.containsAll(localPreviousVersions)) {
          lockMap_.unlock(objectId.toString());
          return QueryToStoreResult.INVALID_VERSION;
        }

        // checking whether local file is up to date
        if (!localPreviousVersions.containsAll(previousVersions)) {
          lockMap_.unlock(objectId.toString());
          return QueryToStoreResult.OBJECT_OUT_OF_DATE;
        }
      } else {
        logger_.debug("storing new file");
      }

      String tmpKey = objectId.toString() + ".tmp." + transactionToken;
      store_.put(tmpKey, encryptedObject.getEncryptedData());
      return QueryToStoreResult.OK;
    } catch (IOException e) {
      lockMap_.unlock(objectId.toString());
      return QueryToStoreResult.SAVE_FAILED;
    }
  }

  public void commitUpdateObject(ObjectId objectId, Set<String> previousVersions,
      String currentVersion, String transactionToken) {
    logger_.debug("Commit storing object " + objectId.toString());

    try {
      String tmpKey = objectId.toString() + ".tmp." + transactionToken;
      byte[] bytes = store_.get(tmpKey);
      store_.put(objectId.toString(), bytes);

      Set<String> newVersions = new HashSet<String>(previousVersions);
      newVersions.add(currentVersion);
      setPreviousVersions(objectId, newVersions);

      logger_.debug("Commit successful");
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to save file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  public void abortUpdateObject(ObjectId objectId, String transactionToken) {
    logger_.debug("Aborting transaction " + objectId.toString());
    try {
      String tmpKey = objectId.toString() + ".tmp." + transactionToken;
      store_.delete(tmpKey);
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to delete file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  /**
   * Retrieves object from disk.
   * @return Encrypted object or null if and only if object can't be read from disk(either because
   * it wasn't stored or there was a problem reading file).
   */
  public EncryptedObject getObject(ObjectId objectId) {
    logger_.debug("getObject with objectID = " + objectId);
    byte[] bytes = store_.get(objectId.toString());

    if (bytes == null) {
      return null;
    } else {
      return new EncryptedObject(bytes);
    }
  }

  public void deleteObject(ObjectId objectId) throws DeleteObjectException {
    try {
      if (!lockMap_.tryLock(objectId.toString(), UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId.toString() + " lock timeout in deleteObject().");
        throw new DeleteObjectException("Timeout while waiting for object lock.");
      }
    } catch (InterruptedException e) {
      logger_.warn("Interrupted while waiting for object lock in deleteObject()");
      throw new DeleteObjectException("Interrupted while waiting for object lock.", e);
    }

    try {
      store_.delete(objectId.toString());
      store_.delete(objectId.toString() + METADATA_SUFFIX);
    } catch (IOException e) {
      throw new DeleteObjectException("Unable to delete file.", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private Set<String> getPreviousVersions(ObjectId objectId) throws IOException {
    byte[] bytes = store_.get(objectId.toString() + METADATA_SUFFIX);
    if (bytes == null) {
      return null;
    } else {
      return Sets.newHashSet(Splitter.on(",").split(new String(bytes, Charsets.UTF_8)));
    }
  }

  private void setPreviousVersions(ObjectId objectId, Set<String> versions) throws IOException {
    String joined = Joiner.on(",").join(versions);
    store_.put(objectId.toString() + METADATA_SUFFIX, joined.getBytes(Charsets.UTF_8));
  }
}
