package org.nebulostore.systest.async;

import org.nebulostore.conductor.CaseStatistics;

/**
 * Case statistics for asynchronous messages test.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestStatistics extends CaseStatistics {

  private static final long serialVersionUID = -9005773569606647041L;

  private final int counter_;

  public AsyncTestStatistics(int counter) {
    counter_ = counter;
  }

  public int getCounter() {
    return counter_;
  }

}
