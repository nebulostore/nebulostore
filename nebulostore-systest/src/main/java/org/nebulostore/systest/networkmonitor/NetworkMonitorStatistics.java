package org.nebulostore.systest.networkmonitor;

import java.util.LinkedList;
import java.util.List;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.utils.Pair;

/**
 * Statistics gathered by the test.
 * @author szymonmatejczyk
 */
public class NetworkMonitorStatistics extends CaseStatistics {
  private static final long serialVersionUID = -6510146532872161217L;
  private final List<Pair<CommAddress, Double>> estimatedAvailability_ =
      new LinkedList<Pair<CommAddress, Double>>();

  public List<Pair<CommAddress, Double>> getEstimatedAvailabilities() {
    return estimatedAvailability_;
  }

  public void addEstimatedAvailability(CommAddress nebuloAddress, Double availability) {
    estimatedAvailability_.add(new Pair<CommAddress, Double>(nebuloAddress, availability));
  }
}
