package org.nebulostore.appcore.model;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.replicator.core.TransactionAnswer;

public interface GeneralObjectWriter {

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
