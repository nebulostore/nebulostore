package org.nebulostore.coding.repetition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ReplicaPlacementData;
import org.nebulostore.coding.ReplicaPlacementPreparator;
import org.nebulostore.communication.naming.CommAddress;

public class RepetitionReplicaPlacementPreparator implements ReplicaPlacementPreparator {

  private static Logger logger_ = Logger.getLogger(RepetitionReplicaPlacementPreparator.class);

  @Override
  public ReplicaPlacementData prepareObject(byte[] encryptedObject,
      Collection<CommAddress> replicatorsAddresses) {
    Map<CommAddress, EncryptedObject> placementMap = new HashMap<>();
    for (CommAddress replicator : replicatorsAddresses) {
      placementMap.put(replicator, new EncryptedObject(encryptedObject));
    }
    logger_.debug("Placement map: " + placementMap);
    return new ReplicaPlacementData(placementMap);
  }

}
