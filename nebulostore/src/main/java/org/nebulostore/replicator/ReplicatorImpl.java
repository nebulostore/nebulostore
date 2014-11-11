package org.nebulostore.replicator;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetEncryptedObjectModule;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.appcore.model.NebuloList;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.DeleteObjectException;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.AppendElementsMessage;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;
import org.nebulostore.replicator.messages.GetListMessage;
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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.name.Named;

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

  private static LockMap lockMap_ = new LockMap();

  private final KeyValueStore<byte[]> store_;
  private final MessageVisitor<Void> visitor_ = new ReplicatorVisitor();

  @Inject
  public ReplicatorImpl(@Named("ReplicatorStore") KeyValueStore<byte[]> store) {
    super(getOrCreateStoredObjectsIndex(store));
    store_ = store;
  }

  private static Set<String> getOrCreateStoredObjectsIndex(KeyValueStore<byte[]> store) {
    try {
      final byte[] empty = toJson(new HashSet<String>());
      store.performTransaction(INDEX_ID, new Function<byte[], byte[]>() {
        @Override
        public byte[] apply(byte[] existing) {
          return existing == null ? empty : existing;
        }
      });
    } catch (IOException e) {
      logger_.error("Could not initialize index!", e);
      return new HashSet<>();
    }
    return fromJson(store.get(INDEX_ID));
  }

  private static byte[] toJson(Set<String> set) {
    Gson gson = new Gson();
    return gson.toJson(set).getBytes(Charsets.UTF_8);
  }

  private static Set<String> fromJson(byte[] json) {
    Gson gson = new Gson();
    return gson.fromJson(new String(json, Charsets.UTF_8),
        new TypeToken<Set<String>>() { } .getType());
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
    
    public Void visit(AppendElementsMessage message) {
      ObjectId listId = message.getListId();

      logger_.debug("Acquiring lock for NebuloList with ObjectId = " + listId);
      if (!acquireObjectLock(listId)) {
        sendUnsuccessfulAppendMessage("Acquiring object lock wasn't successful.", message);
        return null;
      }
      
      logger_.debug("Retrieving NebuloList with ObjectId = " + listId);
      NebuloList list = getList(listId);
      if (list == null) {
        sendUnsuccessfulAppendMessage("Couldn't retrieve list from disk.", message);
        lockMap_.unlock(listId.toString());
        return null;
      }

      List<NebuloElement> elements = message.getElementsToAppend();
      logger_.debug("Appending " + elements.size() + " elements to the list." +
          " Current epoch's end: " + list.getEpochEnd());
      list.localAppend(elements);

      logger_.debug("Putting updated list back to the store.");
      try {
        // TODO just serialize
        byte[] serializedList = CryptoUtils.encryptObject(list).getEncryptedData();
        store_.put(listId.toString(), serializedList);
      } catch (CryptoException | IOException exception) {
        sendUnsuccessfulAppendMessage("Error while storing list back on disk.", message);
        lockMap_.unlock(listId.toString());
        return null;
      }
      logger_.debug("Unlocking object " + listId);

      lockMap_.unlock(listId.toString());

      if (message.shouldPropagate()) {
        ConfirmationMessage confirmation = new ConfirmationMessage(message.getSourceJobId(),
            message.getSourceAddress());
        networkQueue_.add(confirmation);
        logger_.debug("Propagating list update to other replicators.");
        propagateAppendToReplicators(message);
      }
      return null;
    }

    public Void visit(GetListMessage message) {
      logger_.debug("GetListMessage with objectID = " + message.getObjectId());

      EncryptedObject serialized = getObject(message.getObjectId());
      if (serialized == null) {
       logger_.debug("Could not retrieve given list. Dying with error.");
       dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to retrieve list.");
        return null;
      }

      NebuloList storedList;
      try {
        // TODO Just deserialize.
        storedList = (NebuloList) CryptoUtils.decryptObject(serialized);
      } catch (CryptoException exception) {
        logger_.debug("Got exception while deserizalizing list. Dying with error.");
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to deserialize list.");
        return null;
      }


      NebuloList resultList;
      if (message.hasRange() || message.hasPredicate()) {
        int fromIndex = message.getRange().getFirst();
        int toIndex = message.getRange().getSecond();
        
        List<NebuloElement> elementsWithinRange;
        if (message.hasRange()) {
          elementsWithinRange = storedList.getElements(fromIndex, toIndex);
        } else {
          elementsWithinRange = storedList.getAllElements();
        }

        List<NebuloElement> filteredElements;
        if (message.hasPredicate()) {
          filteredElements = (List<NebuloElement>) Collections2.filter(elementsWithinRange,
              message.getPredicate());
        } else {
          filteredElements = elementsWithinRange;
        }

        resultList = new NebuloList(storedList.getAddress(), filteredElements, fromIndex, toIndex,
            message.getPredicate(), storedList.isPublicReadable(), storedList.isPublicAppendable(),
            storedList.isDelible());
      } else {
        resultList = storedList;
      }

      try {
        // TODO Deserialize instead of decrypt.
        EncryptedObject serializedToSend = CryptoUtils.encryptObject(resultList);
        networkQueue_.add(new SendObjectMessage(message.getSourceJobId(), message
            .getSourceAddress(), serializedToSend, null));
      } catch (CryptoException exception) {
        logger_.debug("Got exception while serizalizing list. Dying with error.");
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to serialize list.");
        return null;
      }
        endJobModule();
      return null;
    }

    private void sendUnsuccessfulAppendMessage(String errorInfo, AppendElementsMessage message) {
      if (message.shouldPropagate()) {
        ReplicatorErrorMessage errorMessage = 
            new ReplicatorErrorMessage(message.getId(), message.getSourceAddress(), errorInfo);
        networkQueue_.add(errorMessage);
      }
    }

    /**
     * Retrieves and deserializes NebuloList from disk.
     * 
     * @return Deserialized list or null if object can't be deserialized or read from disk.
     */
    // NOTE: NebuloList data structure won't be stored as encrypted data structure.
    // TODO Once encryption is implemented, this method will need improvement.
    private NebuloList getList(ObjectId listId) {
      EncryptedObject encryptedList = getObject(listId);
      if (encryptedList == null) {
        logger_.warn("There is no list with Id = " + listId + " stored.");
        return null;
      }
      
      NebuloList decryptedList;
      try {
        decryptedList = (NebuloList) CryptoUtils.decryptObject(encryptedList);
      } catch (CryptoException exception) {
        logger_.warn("Got exception while deserializing NebuloList.");
        return null;
      }

      return decryptedList;
    }

    private void propagateAppendToReplicators(AppendElementsMessage appendMsg) {
      Set<CommAddress> replicators = appendMsg.getReplicators().getReplicatorSet();
      replicators.remove(appendMsg.getDestinationAddress());

      ObjectId listId = appendMsg.getListId();
      List<NebuloElement> elementsToAppend = new LinkedList<NebuloElement>(appendMsg.getElementsToAppend());
      
      for (CommAddress replicator: replicators) {
        AppendElementsMessage appendPropagationMsg =
            new AppendElementsMessage(replicator, listId, elementsToAppend, "");
        networkQueue_.add(appendPropagationMsg);
      }
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

      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
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
      String objId = objectId.toString();
      String tmpKey = objId + TMP_SUFFIX + transactionToken;
      byte[] bytes = store_.get(tmpKey);
      store_.put(objId, bytes);
      addToIndex(objId);

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
      String tmpKey = objectId.toString() + TMP_SUFFIX + transactionToken;
      store_.delete(tmpKey);
      removeFromIndex(objectId.toString());
    } catch (IOException e) {
      // TODO: dirty state here
      logger_.warn("unable to delete file", e);
    } finally {
      lockMap_.unlock(objectId.toString());
    }
  }

  private void addToIndex(String objId) {
    storedObjectsIds_.add(objId);
    saveIndex();
  }

  private void removeFromIndex(String objId) {
    storedObjectsIds_.remove(objId);
    saveIndex();
  }

  private void saveIndex() {
    try {
      store_.put(INDEX_ID, toJson(storedObjectsIds_));
    } catch (IOException e) {
      logger_.error("Could not save index!", e);
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
      removeFromIndex(objectId.toString());
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

  private boolean acquireObjectLock(ObjectId objectId) {
    try {
      if (!lockMap_.tryLock(objectId.toString(), UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Lock timeout for Object " + objectId + " in acquireObjectLock().");
        return false;
      }
    } catch (InterruptedException exception) {
      logger_.warn("Interrupted while waiting for object lock in acquireObjectLock().");
      return false;
    }

    return true;
  }

}
