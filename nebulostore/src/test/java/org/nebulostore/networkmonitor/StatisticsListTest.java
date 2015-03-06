package org.nebulostore.networkmonitor;

import junit.framework.Assert;

import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

public class StatisticsListTest {

  private static final double WEIGHT_SINGLE = 0.7;
  private static final double WEIGHT_MULTI = 0.4;
  private static final int MAX_SIZE = 10;

  private PeerConnectionSurvey newLatencyPeerConnectionSurvey(double latency) {
    return new PeerConnectionSurvey(CommAddress.getZero(), 0, ConnectionAttribute.LATENCY, latency);
  }

  /**
   * Fill statistics list with consecutive numbers starting from 1.
   *
   * @param list
   * @param elementsNumber
   */
  private void fillStatisticsList(StatisticsList list, int elementsNumber) {
    for (int i = 0; i < elementsNumber; i++) {
      list.add(newLatencyPeerConnectionSurvey(i + 1));
    }
  }

  @Test
  public void shouldNotExceedLimit() {
    StatisticsList list = new StatisticsList(WEIGHT_SINGLE, WEIGHT_MULTI, MAX_SIZE);
    fillStatisticsList(list, MAX_SIZE + 1);

    Assert.assertEquals(10, list.size());
    double mean = 1.0;
    double sum = 0.0;
    for (int i = 1; i < MAX_SIZE + 1; i++) {
      sum += i + 1;
    }

    mean = mean * WEIGHT_MULTI + sum / MAX_SIZE * (1.0 - WEIGHT_MULTI);
    Assert.assertEquals(mean, list.calcWeightedMean(ConnectionAttribute.LATENCY));
  }

  @Test
  public void shouldMergeListsProperly() {
    StatisticsList list = new StatisticsList(WEIGHT_SINGLE, WEIGHT_MULTI, MAX_SIZE);
    StatisticsList secondList = new StatisticsList(WEIGHT_SINGLE, WEIGHT_MULTI, MAX_SIZE);

    fillStatisticsList(list, MAX_SIZE + 1);
    fillStatisticsList(secondList, MAX_SIZE + 1);

    secondList.addAllInFront(list);

    StatisticsList testList = new StatisticsList(WEIGHT_SINGLE, WEIGHT_MULTI, MAX_SIZE);
    testList.add(newLatencyPeerConnectionSurvey(1));
    for (PeerConnectionSurvey pcs : list.getStatistics(ConnectionAttribute.LATENCY)) {
      testList.add(newLatencyPeerConnectionSurvey(pcs.getValue()));
    }

    for (PeerConnectionSurvey pcs : secondList.getStatistics(ConnectionAttribute.LATENCY)) {
      testList.add(newLatencyPeerConnectionSurvey(pcs.getValue()));
    }

    Assert.assertEquals(secondList, testList);
  }

}
