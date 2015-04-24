package org.nebulostore.coding;

import java.util.HashMap;
import java.util.Map;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Data connected with replica placement for a concrete object.
 *
 * @author Piotr Malicki
 *
 */
public class ReplicaPlacementData {
  private final Map<CommAddress, EncryptedObject> replicaPlacementMap_ = new HashMap<>();

  public ReplicaPlacementData(Map<CommAddress, EncryptedObject> replicaPlacementMap) {
    replicaPlacementMap_.putAll(replicaPlacementMap);
  }

  public Map<CommAddress, EncryptedObject> getReplicaPlacementMap() {
    return replicaPlacementMap_;
  }
}
