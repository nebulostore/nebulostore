package org.nebulostore.appcore.model;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Interface for modules capable of fetching NebuloObjects from the system.
 * @author Bolek Kulbabinski
 */
public interface ObjectGetter {
  /**
   * Fetch the object from NebuloStore asynchronously.
   * @param address NebuloAddress of object that is going to be fetched.
   */
  void fetchObject(NebuloAddress address);

  /**
   * Blocking method that waits for the result of fetchObject().
   * @param timeoutSec Max time in seconds to wait for the result.
   * @return Fetched object.
   * @throws NebuloException
   */
  NebuloObject awaitResult(int timeoutSec) throws NebuloException;
}
