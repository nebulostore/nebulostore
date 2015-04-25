package org.nebulostore.replicator.repairer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetObjectFragmentsModule;
import org.nebulostore.api.GetObjectFragmentsModule.FragmentsData;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.broker.ReplicatorData;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.core.TransactionAnswer;

/**
 * @author Piotr Malicki
 */
public class ReplicaRepairerModule extends ReturningJobModule<Void> {

  private static final int FETCH_OBJECT_TIMEOUT_MILIS = 5000;
  private static final int WRITE_OBJECT_TIMEOUT_MILIS = 5000;
  private static final int WRITE_FINISH_TIMEOUT_MILIS = 1000;

  private static Logger logger_ = Logger.getLogger(ReplicaRepairerModule.class);

  private final AppKey appKey_;
  private final EncryptionAPI encryption_;
  private final String publicKeyPeerId_;
  private final List<CommAddress> currentReplicators_;
  private final List<ReplicatorData> newReplicators_;
  private final Collection<ObjectId> objectIds_;
  private final Provider<ObjectGetter> getModuleProvider_;
  private final Provider<PartialObjectWriter> writeModuleProvider_;
  private final ReplicaPlacementPreparator placementPreparator_;

  protected MessageVisitor visitor_;

  @Inject
  public ReplicaRepairerModule(
      @Assisted("CurrentReplicators") List<CommAddress> currentReplicators,
      @Assisted("NewReplicators") List<ReplicatorData> newReplicators,
      @Assisted Collection<ObjectId> objectIds, Provider<ObjectGetter> getModuleProvider,
      Provider<PartialObjectWriter> writeModuleProvider, AppKey appKey, EncryptionAPI encryption,
      @Named("PublicKeyPeerId") String publicKeyPeerId,
      ReplicaPlacementPreparator placementPreparator) {
    currentReplicators_ = currentReplicators;
    newReplicators_ = newReplicators;
    objectIds_ = objectIds;
    getModuleProvider_ = getModuleProvider;
    writeModuleProvider_ = writeModuleProvider;
    appKey_ = appKey;
    encryption_ = encryption;
    publicKeyPeerId_ = publicKeyPeerId;
    placementPreparator_ = placementPreparator;
    visitor_ = new ReplicaRepairerVisitor();
  }

  protected class ReplicaRepairerVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Starting a repair with parameters:" +
          "\ncurrent replicators: " + currentReplicators_ +
          "\nnew replicators: " + newReplicators_ +
          "\nobjectIds: " + objectIds_);

      Map<CommAddress, Integer> replicatorsToReplaceMap = new HashMap<>();
      // find the replicators we need to replace
      for (ReplicatorData replicatorData : newReplicators_) {
        int sequentialNumber = replicatorData.getSequentialNumber();
        CommAddress replicator = currentReplicators_.get(sequentialNumber);
        replicatorsToReplaceMap.put(replicator, sequentialNumber);
      }

      Map<ObjectId, FragmentsData> fragmentsMap = getFragments(replicatorsToReplaceMap);
      Map<ObjectId, NebuloObject> objects = getObjects(fragmentsMap, replicatorsToReplaceMap);

      if (objects != null) {
        writeObjects(objects, fragmentsMap);
        endWithSuccess(null);
      }
    }

    private Map<ObjectId, FragmentsData> getFragments(
        Map<CommAddress, Integer> replicatorsToReplaceMap) {

      Map<ObjectId, FragmentsData> fragmentsMap = new HashMap<>();
      Map<ObjectId, GetObjectFragmentsModule> modulesMap = new HashMap<>();

      for (ObjectId objectId : objectIds_) {
        NebuloAddress address = new NebuloAddress(appKey_, objectId);
        // Download list of needed fragments with their versions
        GetObjectFragmentsModule module = new GetObjectFragmentsModule(address,
            replicatorsToReplaceMap.keySet(), outQueue_);
        module.runThroughDispatcher();
        modulesMap.put(objectId, module);
      }

      for (Entry<ObjectId, GetObjectFragmentsModule> entry : modulesMap.entrySet()) {
        try {
          FragmentsData data = entry.getValue().getResult(FETCH_OBJECT_TIMEOUT_MILIS);
          fragmentsMap.put(entry.getKey(), data);
        } catch (NebuloException e) {
          logger_.warn("Error in GetObjectFragmentsModule for object: " + entry.getKey(), e);
        }
      }

      return fragmentsMap;
    }

    private Map<ObjectId, NebuloObject> getObjects(Map<ObjectId, FragmentsData> fragmentsMap,
        Map<CommAddress, Integer> replicatorsToReplaceMap) {
      Map<ObjectId, NebuloObject> objects = new HashMap<>();

      for (ObjectId objectId : objectIds_) {
        FragmentsData fragmentsData = fragmentsMap.get(objectId);
        if (fragmentsData == null ||
            fragmentsData.fragmentsMap_.size() < replicatorsToReplaceMap.size()) {
          ObjectGetter getModule = getModuleProvider_.get();
          getModule.fetchObject(new NebuloAddress(appKey_, objectId));
          try {
            objects.put(objectId, getModule.awaitResult(FETCH_OBJECT_TIMEOUT_MILIS));
          } catch (NebuloException e) {
            endWithError(new NebuloException("Error while trying to retrieve object " +
                objectId, e));
            return null;
          }
        }
      }
      return objects;
    }

    private void writeObjects(Map<ObjectId, NebuloObject> objects,
        Map<ObjectId, FragmentsData> fragmentsMap) {
      Set<PartialObjectWriter> writeModules = new HashSet<>();

      for (ObjectId objectId : objectIds_) {
        PartialObjectWriter writeModule = writeModuleProvider_.get();
        Map<CommAddress, EncryptedObject> objectsMap = new HashMap<>();
        if (objects.containsKey(objectId)) {
          // We can use the whole object because it is in its newest possible version
          NebuloObject object = objects.get(objectId);
          EncryptedObject encryptedObject;
          try {
            encryptedObject = encryption_.encrypt(object, publicKeyPeerId_);
          } catch (CryptoException e) {
            endWithError(new NebuloException("Error in object encryption", e));
            return;
          }

          ReplicaPlacementData placementData =
              placementPreparator_.prepareObject(encryptedObject.getEncryptedData(),
                  currentReplicators_);

          for (ReplicatorData replicatorData : newReplicators_) {
            putFragment(replicatorData, placementData.getReplicaPlacementMap(), objectsMap);
          }

          writeModule.writeObject(objectsMap, object.getVersions(), newReplicators_.size(),
              object.getObjectId());
        } else {
          FragmentsData fragmentsData = fragmentsMap.get(objectId);
          for (ReplicatorData replicatorData : newReplicators_) {
            putFragment(replicatorData, fragmentsData.fragmentsMap_, objectsMap);
          }

          writeModule.writeObject(objectsMap, fragmentsData.versions_, newReplicators_.size(),
              objectId);

          writeModules.add(writeModule);
        }
      }

      try {
        for (PartialObjectWriter writeModule : writeModules) {
          writeModule.getSemiResult(WRITE_OBJECT_TIMEOUT_MILIS);
        }
      } catch (NebuloException e) {
        setWriteModulesAnswer(writeModules, TransactionAnswer.ABORT);
        endWithError(new NebuloException("One of write modules failed, transaction aborted."));
        return;
      }

      setWriteModulesAnswer(writeModules, TransactionAnswer.COMMIT);

      for (PartialObjectWriter writeModule : writeModules) {
        try {
          writeModule.awaitResult(WRITE_FINISH_TIMEOUT_MILIS);
        } catch (NebuloException e) {
          logger_.warn("Error while waiting for finishing write module: " + writeModule, e);
        }
      }

      logger_.debug("Repair ended successfully.");
    }

    private void putFragment(ReplicatorData replicatorData, Map<CommAddress,
        EncryptedObject> fragmentsMap, Map<CommAddress, EncryptedObject> objectsMap) {
      CommAddress previousReplicator =
          currentReplicators_.get(replicatorData.getSequentialNumber());
      CommAddress replicator = replicatorData.getContract().getPeer();
      objectsMap.put(replicator, fragmentsMap.get(previousReplicator));

    }

    private void setWriteModulesAnswer(Set<PartialObjectWriter> writeModules,
        TransactionAnswer answer) {
      for (PartialObjectWriter writeModule : writeModules) {
        writeModule.setAnswer(answer);
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
