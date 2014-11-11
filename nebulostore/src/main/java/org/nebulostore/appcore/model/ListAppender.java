package org.nebulostore.appcore.model;

import java.util.List;

import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Interface for modules capable of appending elements to NebuloLists.
 */
public interface ListAppender {

  /**
   * Append elements asynchronously.
   * 
   * @param list List to append to.
   * @param elementsToAppend Elements to append.
   */
  void appendElements(NebuloList list, List<NebuloElement> elementsToAppend);


  /**
   * Blocking method that waits for the result of fetchList().
   * 
   * @param timeoutSec Max time in seconds to wait for the result.
   * @throws NebuloException
   */
  void awaitResult(int timeoutSec) throws NebuloException;
}