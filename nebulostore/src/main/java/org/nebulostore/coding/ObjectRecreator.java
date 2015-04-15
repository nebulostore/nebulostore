package org.nebulostore.coding;

import java.util.Collection;
import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Interface for object recreators that are used to recreate an object based on its fragments
 * downloaded from other peers. Recreation is done using proper coding algorithm.
 *
 * @author Piotr Malicki
 *
 */
public interface ObjectRecreator {

  /**
   * Set replicators to ask for object fragments - the order of replicators in given collection
   * is important.
   *
   * @param replicators
   */
  void setReplicators(Collection<CommAddress> replicators);

  /**
   * Add next file fragment to the recreator. This method returns true if the recreator has already
   * received enough fragments to recreate the object false otherwise.
   *
   * @param fragment
   *          next fragment of the object
   * @param replicator
   *          address of peer which sent this fragment
   * @return
   */
  boolean addNextFragment(EncryptedObject fragment, CommAddress replicator);

  /**
   * Recreate the object based on received fragments and return it. This method should be called
   * only after addNextFragment method returned true at least once. In the other case, an exception
   * is thrown.
   *
   * @return recreated object
   */
  EncryptedObject recreateObject() throws NebuloException;

  int getNeededFragmentsNumber();

  /**
   * Calculate replicators to ask for object fragments. The returned replicators list should be as
   * short as it's possible.
   *
   * @return list of replicators to ask
   */
  List<CommAddress> calcReplicatorsToAsk();

  /**
   * Remove replicator from possible peers to ask for fragment. This method should be used when the
   * caller didn't manage to receive object fragment from given peer.
   *
   * @param replicator
   */
  void removeReplicator(CommAddress replicator);


}
