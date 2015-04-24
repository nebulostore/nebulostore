package org.nebulostore.coding;

import java.util.Collection;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Interface for replica placement preparators.
 *
 * @author Piotr Malicki
 *
 */
public interface ReplicaPlacementPreparator {

  /**
   * Encode given object and prepare placement data informing at which replicator each part should
   * be placed.
   *
   * @param encryptedObject object to place at replicators
   * @param replicatorsAddresses addresses of all object's replicators
   * @return
   */
  ReplicaPlacementData prepareObject(byte[] encryptedObject,
      Collection<CommAddress> replicatorsAddresses);
}
