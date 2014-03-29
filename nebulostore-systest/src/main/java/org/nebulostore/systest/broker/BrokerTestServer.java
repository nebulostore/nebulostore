package org.nebulostore.systest.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.nebulostore.broker.Contract;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Starts 2 peers and runs NetworkMonitor module.
 *
 *
 * @author szymonmatejczyk
 *
 */
public class BrokerTestServer extends ConductorServer {
  private static Logger logger_ = Logger.getLogger(BrokerTestServer.class);

  private static final int NUM_PHASES = 2;
  private static final int TIMEOUT_SEC = 200;
  private static final int INITIAL_SLEEP = 1000;


  /**
   * Availabilities clients will simulating.
   */
  private final Map<CommAddress, Double> clientsAvailabilities_;


  public BrokerTestServer() {
    super(NUM_PHASES, TIMEOUT_SEC, "BrokerClient_" + CryptoUtils.getRandomString(),
        "Broker test");
    gatherStats_ = true;
    clientsAvailabilities_ = new HashMap<CommAddress, Double>();
  }

  private static final double[] AVAILABILITIES = {0.9, 0.6, 0.5, 0.4, 0.3, 0.1};

  @Override
  public void initClients() {
    sleep(INITIAL_SLEEP);
    Iterator<CommAddress> it = clients_.iterator();
    List<CommAddress> clients = new ArrayList<CommAddress>(peersNeeded_);
    for (int i = 0; i < peersNeeded_; ++i)
      clients.add(it.next());
    double availability;
    for (int i = 0; i < peersNeeded_; ++i) {
      availability = AVAILABILITIES[i];
      logger_.info(String.format("Peer %s availability: %.2f", clients.get(i).toString(),
          availability));
      clientsAvailabilities_.put(clients.get(i), availability);
      networkQueue_.add(new InitMessage(clientsJobId_, null, clients.get(i),
          new BrokerTestClient(jobId_, NUM_PHASES, commAddress_, availability)));
    }
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics statistics) {
    logger_.info("Received stats from " + sender);
    BrokerTestStatistics stats = (BrokerTestStatistics) statistics;
    for (Contract contract : stats.getContracts()) {
      logger_.info(contract.toString());
    }
  }

  @Override
  protected String getAdditionalStats() {
    return null;
  }
}
