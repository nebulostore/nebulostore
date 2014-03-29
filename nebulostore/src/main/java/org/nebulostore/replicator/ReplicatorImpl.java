package org.nebulostore.replicator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetEncryptedObjectModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
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
import org.nebulostore.utils.Pair;

/**
 * Replicator - disk interface.
 * @author szymonmatejczyk
 */
public class ReplicatorImpl extends Replicator {
  private static Logger logger_ = Logger.getLogger(ReplicatorImpl.class);

  private static final int UPDATE_TIMEOUT_SEC = 10;
  private static final int LOCK_TIMEOUT_SEC = 10;
  private static final int GET_OBJECT_TIMEOUT_SEC = 10;

  private static Map<ObjectId, Semaphore> locksMap_ = new Hashtable<ObjectId, Semaphore>();

  private String pathPrefix_;
  private AppKey appKey_;
  private final MessageVisitor<Void> visitor_ = new ReplicatorVisitor();

  @Inject
  public ReplicatorImpl(@Named("replicator.storage-path") String pathPrefix,
                        AppKey appKey) {
    pathPrefix_ = pathPrefix;
    appKey_ = appKey;
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
      jobId_ = message.getId();

      QueryToStoreResult result = queryToUpdateObject(message.getObjectId(),
          message.getEncryptedEntity(), message.getPreviousVersionSHAs(), message.getId());
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
      jobId_ = message.getId();
      EncryptedObject enc = getObject(message.getObjectId());
      Set<String> versions = getPreviousVersions(message.getObjectId());

      if (enc == null) {
        logger_.debug("Could not retrieve given object. Dying with error.");
        dieWithError(message.getSourceJobId(), message.getDestinationAddress(),
            message.getSourceAddress(), "Unable to retrieve object.");
      } else {
        networkQueue_.add(new SendObjectMessage(message.getSourceJobId(),
            message.getSourceAddress(), enc, versions));
      }
      endJobModule();
      return null;
    }

    public Void visit(DeleteObjectMessage message) {
      jobId_ = message.getId();
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
      jobId_ = message.getId();
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

    String location = getObjectLocation(objectId);
    if (objectExists(location)) {

      Set<String> localPreviousVersions = getPreviousVersions(objectId);

      /* checking whether remote file is up to date (update is not concurrent) */
      if (!previousVersions.containsAll(localPreviousVersions)) {
        return QueryToStoreResult.INVALID_VERSION;
      }

      /* checking whether local file is up to date */
      if (!localPreviousVersions.containsAll(previousVersions)) {
        return QueryToStoreResult.OBJECT_OUT_OF_DATE;
      }

    } else {
      logger_.debug("storing new file");
      // TODO(szm, bolek): addtional synchronization for locksMap_?
      if (locksMap_.get(objectId) == null) {
        locksMap_.put(objectId, new Semaphore(1));
      }
    }

    try {
      Semaphore lock = locksMap_.get(objectId);
      if (lock == null) {
        // TODO(bolek): Better answer here?
        return QueryToStoreResult.TIMEOUT;
      }
      if (!lock.tryAcquire(UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId.toString() + " lock timeout in queryToUpdateObject().");
        return QueryToStoreResult.TIMEOUT;
      }
    } catch (InterruptedException exception) {
      logger_.warn("Interrupted while waiting for object lock in queryToUpdateObject()");
      return QueryToStoreResult.TIMEOUT;
    }

    String tmpLocation = location + ".tmp." + transactionToken;
    File f = new File(tmpLocation);
    f.getParentFile().mkdirs();
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(f);
    } catch (FileNotFoundException e1) {
      logger_.error("Could not open stream in queryToUpdateObject().");
      return QueryToStoreResult.SAVE_FAILED;
    }

    try {
      fos.write(encryptedObject.getEncryptedData());
      logger_.debug("File written to tmp location");
    } catch (IOException exception) {
      logger_.error(exception.getMessage());
      return QueryToStoreResult.SAVE_FAILED;
    } finally {
      try {
        fos.close();
      } catch (IOException e) {
        logger_.error("Could not close stream in queryToUpdateObject().");
        return QueryToStoreResult.SAVE_FAILED;
      }
    }

    return QueryToStoreResult.OK;
  }

  public void commitUpdateObject(ObjectId objectId, Set<String> previousVersions,
      String currentVersion, String transactionToken) {
    logger_.debug("Commit storing object " + objectId.toString());

    String location = getObjectLocation(objectId);
    File previous = new File(location);
    previous.delete();
    File tmp = new File(location + ".tmp." + transactionToken);
    tmp.renameTo(previous);

    Set<String> newVersions = new HashSet<String>(previousVersions);
    newVersions.add(currentVersion);
    setPreviousVersions(objectId, newVersions);

    Semaphore lock = locksMap_.get(objectId);
    if (lock != null) {
      lock.release();
    }
    logger_.debug("Commit successful");
  }

  public void abortUpdateObject(ObjectId objectId, String transactionToken) {
    logger_.debug("Aborting transaction " + objectId.toString());
    String location = getObjectLocation(objectId);
    boolean newObjectTransaction = objectExists(location);

    File file = new File(location + ".tmp." + transactionToken);
    file.delete();
    Semaphore lock = locksMap_.get(objectId);
    if (lock != null) {
      lock.release();
    }
    if (newObjectTransaction) {
      // New local object wasn't created.
      locksMap_.remove(objectId);
    }
  }

  /**
   * Retrieves object from disk.
   * @return Encrypted object or null if and only if object can't be read from disk(either because
   * it wasn't stored or there was a problem reading file).
   */
  public EncryptedObject getObject(ObjectId objectId) {
    logger_.debug("getObject with objectID = " + objectId);
    String location = getObjectLocation(objectId);
    if (!objectExists(location)) {
      logger_.debug("Given object does not exist at location: " + location + ".");
      return null;
    }

    File file = new File(location);
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
    } catch (FileNotFoundException exception) {
      logger_.warn("Object file not found (" + location + ").");
      return null;
    }

    try {
      byte[] content = new byte[(int) (file.length())];
      fis.read(content);
      return new EncryptedObject(content);
    } catch (IOException exception) {
      logger_.warn(exception.toString());
      return null;
    } finally {
      try {
        fis.close();
      } catch (IOException e) {
        logger_.warn("Could not close stream in getObject().");
        return null;
      }
    }
  }

  public void deleteObject(ObjectId objectId) throws DeleteObjectException {
    String location = getObjectLocation(objectId);
    Semaphore mutex = locksMap_.get(objectId);
    if (!objectExists(location) || mutex == null) {
      return;
    }
    try {
      if (!mutex.tryAcquire(UPDATE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        logger_.warn("Object " + objectId.toString() + " lock timeout in deleteObject().");
        throw new DeleteObjectException("Timeout while waiting for object lock.");
      }
    } catch (InterruptedException e) {
      logger_.warn("Interrupted while waiting for object lock in deleteObject()");
      throw new DeleteObjectException("Interrupted while waiting for object lock.", e);
    }

    // TODO(bolek): Remove all temps and metadata related to this file.
    locksMap_.remove(objectId);
    mutex.release();

    File f = new File(location);
    if (!f.exists()) {
      throw new DeleteObjectException("File does not exist.");
    }
    boolean success = f.delete();
    if (!success) {
      throw new DeleteObjectException("Unable to delete file.");
    }
  }

  private Set<String> getPreviousVersions(ObjectId objectId) {
    File file = new File(getObjectLocation(objectId) + ".meta");
    Splitter splitter = Splitter.on(",");
    String line;
    try {
      line = Files.readFirstLine(file, Charsets.US_ASCII);
      return line == null ? new HashSet<String>() : Sets.newHashSet(splitter.split(line));
    } catch (IOException e) {
      return new HashSet<String>();
    }
  }

  private void setPreviousVersions(ObjectId objectId, Set<String> versions) {
    File file = new File(getObjectLocation(objectId) + ".meta");
    Joiner joiner = Joiner.on(",");
    try {
      Files.write(joiner.join(versions), file, Charsets.US_ASCII);
    } catch (IOException e) {
      logger_.warn("IOException in setPreviousVersions. Versions not updated.", e);
    }
  }

  private boolean objectExists(String location) {
    File objectFile = new File(location);
    return objectFile.exists();
  }

  private String getObjectLocation(ObjectId objectId) {
    return getLocationPrefix() + objectId.getKey();
  }

  private String getLocationPrefix() {
    return pathPrefix_ + "/" + appKey_.getKey().toString() + "_storage/";
  }
}
