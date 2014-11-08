package org.nebulostore.appcore.model;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.naming.CommAddress;

import com.google.common.base.Predicate;

/**
 * Interface for modules capable of fetching NebuloLists or its sublists from the system.
 */

public interface ListGetter {
  /**
   * Fetch the list from NebuloStore asynchronously.
   * 
   * @param address NebuloAddress of list that is going to be fetched.
   * @param replicaAddress Optionally, CommAddress of the replica to query or null.
   */
  void fetchList(NebuloAddress address, CommAddress replicaAddress);

  /**
   * Fetch the sublist of the NebuloList from NebuloStore asynchronously.
   * 
   * @param address NebuloAddress of the sublist that is going to be fetched.
   * @param fromIndex Low endpoint (inclusive) of the sublist.
   * @param toIndex High endpoint (exclusive) of the sublist.
   * @param replicaAddress Optionally, CommAddress of the replica to query or null.
   */
  void fetchList(NebuloAddress address, int fromIndex, int toIndex, CommAddress replicaAddress);

  /**
   * Fetch the sublist of the NebuloList from NebuloStore asynchronously.
   * 
   * @param address NebuloAddress of the sublist that is going to be fetched.
   * @param predicate Predicate based on which elements of the list will be selected.
   * @param replicaAddress Optionally, CommAddress of the replica to query or null.
   */
  void fetchList(NebuloAddress address, Predicate<NebuloElement> predicate,
      CommAddress replicaAddress);

  /**
   * Fetch the sublist of the NebuloList from NebuloStore asynchronously.
   * 
   * @param address NebuloAddress of the sublist that is going to be fetched.
   * @param fromIndex Low endpoint (inclusive) of the sublist.
   * @param toIndex High endpoint (exclusive) of the sublist.
   * @param predicate Predicate based on which elements of the list will be selected.
   * @param replicaAddress Optionally, CommAddress of the replica to query or null.
   */
  void fetchList(NebuloAddress address,
      int fromIndex, int toIndex, Predicate<NebuloElement> predicate, CommAddress replicaAddress);
  
  /**
   * Blocking method that waits for the result of fetchList().
   * 
   * @param timeoutSec Max time in seconds to wait for the result.
   * @return Fetched list.
   * @throws NebuloException
   */
  NebuloList awaitResult(int timeoutSec) throws NebuloException;
}