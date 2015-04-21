package org.nebulostore.appcore.model;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.replicator.core.TransactionAnswer;

/**
 * Interface for modules capable of writing NebuloObjects.
 * @author Bolek Kulbabinski
 */
public interface ObjectWriter {
  /**
   * Write the object asynchronously.
   * @param objectToWrite
   * @param previousVersionSHAs
   */
  void writeObject(NebuloObject objectToWrite, List<String> previousVersionSHAs);

  // TODO(bolek): Move this logic into the module, it should not be inside NebuloFile!
  Void getSemiResult(int timeout) throws NebuloException;
  void setAnswer(TransactionAnswer answer);

  /**
   * Blocking method that waits for the end of module's execution.
   * @param timeoutSec
   * @throws NebuloException thrown if write was unsuccessful
   */
  void awaitResult(int timeoutSec) throws NebuloException;
}
