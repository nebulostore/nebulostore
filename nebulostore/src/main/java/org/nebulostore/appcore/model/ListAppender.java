package org.nebulostore.appcore.model;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Interface for modules capable of appending elements to NebuloLists.
 */
public interface ListAppender {

  /**
   * Append elements asynchronously.
   */
  void appendElements(NebuloList list, List<NebuloElement> elementsToAppend);

  /**
   * 
   */
  void awaitResult(int timeoutSec) throws NebuloException;

}