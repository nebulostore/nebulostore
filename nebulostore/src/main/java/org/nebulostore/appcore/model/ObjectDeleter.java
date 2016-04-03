package org.nebulostore.appcore.model;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.crypto.session.SessionObjectMap;

/**
 * Interface for modules capable of deleting NebuloObjects from the system.
 * @author Bolek Kulbabinski
 */
public interface ObjectDeleter {
  /**
   * Delete the object asynchronously.
   * @param address NebuloAddress of object that is going to be deleted.
   * @param sessionKeys
   */
  void deleteObject(NebuloAddress address, SessionObjectMap sessionKeys);

  /**
   * Blocking method that waits for the end of module's execution.
   * @param timeoutSec Max time in seconds to wait for the result.
   * @throws NebuloException
   */
  void awaitResult(int timeoutSec) throws NebuloException;
}
