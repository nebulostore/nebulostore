package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import java.io.Serializable;
import java.util.Collection;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Results for this test.
 *
 * @author Grzegorz Milka
 *
 */
public class TestResult implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Collection<CommAddress> failedPings_;
  private final Collection<CommAddress> receivedPongs_;

  public TestResult(Collection<CommAddress> failedPings,
      Collection<CommAddress> receivedPongs) {
    failedPings_ = failedPings;
    receivedPongs_ = receivedPongs;
  }

  public Collection<CommAddress> getFailedPings() {
    return failedPings_;
  }

  public Collection<CommAddress> getReceivedPongs() {
    return receivedPongs_;
  }

  @Override
  public String toString() {
    return "TestResult[Failed pings: " + failedPings_.toString() + ", received pongs: " +
        receivedPongs_.toString() + "]";
  }
}
