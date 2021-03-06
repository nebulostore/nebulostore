package org.nebulostore.replicator;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.crypto.SecretKey;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetEncryptedObjectModule;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.broker.messages.CheckContractMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.DHGetSessionKeyMessage;
import org.nebulostore.crypto.session.message.DHGetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.DHLocalErrorMessage;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.DeleteObjectException;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.StoreData;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.CheckContractResultMessage;
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
  private static final String TMP_SUFFIX = ".tmp.";
  private static final String INDEX_ID = "object.index";

  private enum ActionType { WRITE, READ, DELETE }

  private static LockMap lockMap_ = new LockMap();

  private final KeyValueStore<byte[]> store_;
  private final MessageVisitor visitor_ = new ReplicatorVisitor();
  private EncryptionAPI encryptionAPI_;
  private final Map<String, SessionInnerMessageInterface> workingMessages_ =
        new HashMap<String, SessionInnerMessageInterface>();
  private final Map<String, SecretKey> workingSecretKeys_ =
      new HashMap<String, SecretKey>();
  private final Map<String, ActionType> actionTypes_ = new HashMap<String, ActionType>();


  @Inject
  public ReplicatorImpl(@Named("ReplicatorStore") KeyValueStore<byte[]> store,
                        EncryptionAPI encryptionAPI) {
    super(getOrCreateStoredObjectsIndex(store));
    store_ = store;
    encryptionAPI_ = encryptionAPI;
  }

  public ReplicatorImpl(KeyValueStore<byte[]> store) {
    super(getOrCreateStoredObjectsIndex(store));
    store_ = store;
  }

  private static Map<String, MetaData> getOrCreateStoredObjectsIndex(KeyValueStore<byte[]> store) {
    try {
      final byte[] empty = toJson(new HashMap<String, MetaData>());
      store.performTransaction(INDEX_ID, new Function<byte[], byte[]>() {
        @Override
        public byte[] apply(byte[] existing) {
          return existing == null ? empty : existing;
        }
      });
    } catch (IOException e) {
      logger_.error("Could not initialize index!", e);
      return new HashMap<>();
    }
    return fromJson(store.get(INDEX_ID));
  }

  private static byte[] toJson(Map<String, MetaData> set) {
    Gson gson = new Gson();
    return gson.toJson(set).getBytes(Charsets.UTF_8);
  }

  private static Map<String, MetaData> fromJson(byte[] json) {
    Gson gson = new Gson();
    return gson.fromJson(new String(json, Charsets.UTF_8),
        new TypeToken<Map<String, MetaData>>() { } .getType());
  }

  /**
   * Result of queryToStore.
   */
  private enum QueryToStoreResult { OK, OBJECT_OUT_OF_DATE, INVALID_VERSION, SAVE_FAILED, TIMEOUT }

  /**
   * Visitor to handle different message types.
   * @author szymonmatejczyk
   */
  protected class ReplicatorVisitor extends MessageVisitor {
    private StoreData storeData_;

    private void saveObject(SecretKey sessionKey, QueryToStoreObjectMessage message)
        throws NebuloException {
      EncryptedObject enc = null;
      try {
        enc = (EncryptedObject) encryptionAPI_.decryptWithSessionKey(
            message.getEncryptedEntity(), sessionKey);
      } catch (NullPointerException | CryptoException e) {
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to save object.");
        return;

      }
      QueryToStoreResult result = queryToUpdateObject(message.getObjectId(),
          enc, message.getPreviousVersionSHAs(), message.getId());
      logger_.debug("queryToUpdateObject returned " + result.name());
      switch (result) {
        case OK:
          networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
              message.getSourceAddress()));
          storeData_ = new StoreData(message.getId(), message.getObjectId(), enc,
              message.getPreviousVersionSHAs(), message.getNewVersionSHA());
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
    }

    public void visit(QueryToStoreObjectMessage message) throws NebuloException {
      logger_.debug("StoreObjectMessage received");
      CommAddress peerAddress = message.getSourceAddress();
      workingMessages_.put(message.getSessionId(), message);
      actionTypes_.put(message.getSessionId(), ActionType.WRITE);
      outQueue_.add(new DHGetSessionKeyMessage(peerAddress, getJobId(),
          message.getSessionId()));
    }

    public void visit(TransactionResultMessage message) {
      logger_.debug("TransactionResultMessage received: " + message.getResult());
      if (storeData_ == null) {
        //TODO(szm): ignore late abort transaction messages send by timer.
        logger_.warn("Unexpected commit message received.");
        endJobModule();
        return;
      }
      if (message.getResult() == TransactionAnswer.COMMIT) {
        commitUpdateObject(storeData_.getObjectId(),
                           storeData_.getPreviousVersionSHAs(),
                           storeData_.getNewVersionSHA(),
                           message.getId());
      } else {
        abortUpdateObject(storeData_.getObjectId(), message.getId());
      }
      endJobModule();
    }

    public void visit(GetObjectMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      workingMessages_.put(message.getSessionId(), message);
      actionTypes_.put(message.getSessionId(), ActionType.READ);
      outQueue_.add(new DHGetSessionKeyMessage(peerAddress, getJobId(),
          message.getSessionId()));
    }

    public void visit(DHGetSessionKeyResponseMessage message) {
      workingSecretKeys_.put(message.getSessionId(), message.getSessionKey());
      outQueue_.add(new CheckContractMessage(getJobId(), message.getPeerAddress(),
          message.getSessionId()));
    }

    public void visit(DHLocalErrorMessage message) {
      logger_.debug("InitSessionEndWithErrorMessage " + message.getErrorMessage());
      SessionInnerMessageInterface getObjectMessage =
          workingMessages_.remove(message.getPeerAddress());
      dieWithError(getObjectMessage.getSourceJobId(), getObjectMessage.getDestinationAddress(),
          message.getPeerAddress(), "Unable to retrieve object.");
    }

    private void retrieveObject(CommAddress peerAddress, String sessionId,
        SecretKey sessionKey, GetObjectMessage getObjectMessage) {
      EncryptedObject enc = null;
      try {
        enc = encryptionAPI_.encryptWithSessionKey(
            getObject(getObjectMessage.getObjectId()), sessionKey);
      } catch (NullPointerException | CryptoException e) {
        logger_.debug("Encryption with session key failed!", e);
        dieWithError(getObjectMessage.getSourceJobId(), getObjectMessage.getDestinationAddress(),
            peerAddress, "Unable to retrieve object.");
        return;

      }
      List<String> versions;
      try {
        versions = getPreviousVersions(getObjectMessage.getObjectId());
      } catch (IOException e) {
        logger_.debug("Exception when getting previous versions ", e);
        dieWithError(getObjectMessage.getSourceJobId(), getObjectMessage.getDestinationAddress(),
            peerAddress, "Unable to retrieve object.");
        return;
      }

      networkQueue_.add(new SendObjectMessage(getObjectMessage.getSourceJobId(),
          peerAddress, sessionId, enc, versions));
      endJobModule();
    }

    public void visit(CheckContractResultMessage message) throws NebuloException {
      String sessionId = message.getSessionId();
      SessionInnerMessageInterface insideSessionMessage = workingMessages_.remove(sessionId);
      CommAddress peerAddress = insideSessionMessage.getSourceAddress();
      SecretKey sessionKey = workingSecretKeys_.remove(sessionId);
      ActionType actionType = actionTypes_.remove(sessionId);
      logger_.debug("CheckContractResultMessage Peer " + peerAddress);
      if (!message.getResult()) {
        dieWithError(insideSessionMessage.getSourceJobId(),
            insideSessionMessage.getDestinationAddress(), peerAddress, "CheckContract error.");
        return;
      }
      switch (actionType) {
        case READ:
          retrieveObject(peerAddress, sessionId, sessionKey,
              (GetObjectMessage) insideSessionMessage);
          break;
        case WRITE:
          saveObject(sessionKey, (QueryToStoreObjectMessage) insideSessionMessage);
          break;
        case DELETE:
          processDeleteMessage(sessionKey, (DeleteObjectMessage) insideSessionMessage);
          break;
        default:
          break;
      }
    }

    public void visit(DeleteObjectMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      workingMessages_.put(message.getSessionId(), message);
      actionTypes_.put(message.getSessionId(), ActionType.DELETE);
      outQueue_.add(new DHGetSessionKeyMessage(peerAddress, getJobId(),
          message.getSessionId()));
    }

    public void visit(ObjectOutdatedMessage message) {
      try {
        GetEncryptedObjectModule getModule = new GetEncryptedObjectModule(message.getAddress(),
            outQueue_);
        Pair<EncryptedObject, List<String>> res = getModule.getResult(GET_OBJECT_TIMEOUT_SEC);
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
              message.getCurrentVersion(), message.getId());
        } else {
          throw new NebuloException("Unable to fetch new version of file.");
        }
      } catch (NebuloException exception) {
        logger_.warn(exception);
      }
    }

    private void dieWithError(String jobId, CommAddress sourceAddress,
        CommAddress destinationAddress, String errorMessage) {
      networkQueue_.add(new ReplicatorErrorMessage(jobId, destinationAddress, errorMessage));
      endJobModule();
    }

    private void processDeleteMessage(SecretKey sessionKey, DeleteObjectMessage message)
        throws CryptoException {
      try {
        ObjectId objectId = (ObjectId) encryptionAPI_.decryptWithSessionKey(
            message.getEncryptedData(), sessionKey);
        deleteObject(objectId);
      } catch (DeleteObjectException exception) {
        logger_.warn(exception.toString());
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), exception.getMessage());
      }
      endJobModule();
      networkQueue_.add(new ConfirmationMessage(message.getSourceJobId(),
          message.getSourceAddress()));
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
      EncryptedObject encryptedObject, List<String> previousVersions, String transactionToken) {
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
        List<String> localPreviousVersions = getPreviousVersions(objectId);

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

      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
      store_.put(tmpKey, encryptedObject.getEncryptedData());
      return QueryToStoreResult.OK;
    } catch (IOException e) {
      lockMap_.unlock(objectId.toString());
      return QueryToStoreResult.SAVE_FAILED;
    }
  }

  public void commitUpdateObject(ObjectId objectId, List<String> previousVersions,
      String currentVersion, String transactionToken) {
    logger_.debug("Commit storing object " + objectId.toString());

    try {
      String objId = objectId.toString();
      String tmpKey = objId + TMP_SUFFIX + transactionToken;
      byte[] bytes = store_.get(tmpKey);
      store_.delete(tmpKey);
      store_.put(objId, bytes);
      addToIndex(new MetaData(objId, bytes.length));

      List<String> newVersions = new LinkedList<String>(previousVersions);
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
      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
      store_.delete(tmpKey);
      removeFromIndexbyId(objectId.toString());
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to delete file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private void addToIndex(MetaData metaData) {
    storedObjectsMeta_.put(metaData.getObjectId(), metaData);
    saveIndex();
  }

  private void removeFromIndexbyId(String objId) {
    storedObjectsMeta_.remove(objId);
    saveIndex();
  }

  private void saveIndex() {
    try {
      store_.put(INDEX_ID, toJson(storedObjectsMeta_));
    } catch (IOException e) {
      logger_.error("Could not save index!", e);
    }
  }

  /**
   * Retrieves object from disk.
   * @return Encrypted object or null if and only if object can't be read from disk(either because
   * it wasn't stored or there was a problem reading file).
   */
  private EncryptedObject getObject(ObjectId objectId) {
    logger_.debug("getObject with objectID = " + objectId);
    byte[] bytes = store_.get(objectId.toString());

    if (bytes == null) {
      throw new NullPointerException();
    } else {
      return new EncryptedObject(bytes);
    }
  }

  private void deleteObject(ObjectId objectId) throws DeleteObjectException {
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
      removeFromIndexbyId(objectId.toString());
    } catch (IOException e) {
      throw new DeleteObjectException("Unable to delete file.", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private List<String> getPreviousVersions(ObjectId objectId) throws IOException {
    byte[] bytes = store_.get(objectId.toString() + METADATA_SUFFIX);
    if (bytes == null) {
      return null;
    } else {
      return Lists.newLinkedList(Splitter.on(",").split(new String(bytes, Charsets.UTF_8)));
    }
  }

  private void setPreviousVersions(ObjectId objectId, List<String> versions) throws IOException {
    String joined = Joiner.on(",").join(versions);
    store_.put(objectId.toString() + METADATA_SUFFIX, joined.getBytes(Charsets.UTF_8));
  }
}
