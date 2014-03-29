package org.nebulostore.systest.networkmonitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.utils.Pair;

/**
 * Starts 2 peers and runs NetworkMonitor module.
 *
 * Every peers simulates lower availability using injected ConnectionTestsMessageHandler that
 * responds only to some of the queries. At the end Server checks if their estimated availabilities
 * are correct.
 *
 * @author szymonmatejczyk
 *
 */
public class NetworkMonitorTestServer extends ConductorServer {
  private static Logger logger_ = Logger.getLogger(NetworkMonitorTestServer.class);

  private static final int NUM_PHASES = 1;
  private static final int TIMEOUT_SEC = 150;
  private static final int INITIAL_SLEEP = 1000;

  private static final double ESTIMATION_PRECISION = 0.15;

  /**
   * Availabilities clients will simulating.
   */
  private final Map<CommAddress, Double> clientsAvailabilities_;

  private final Random random_ = new Random();

  public NetworkMonitorTestServer() {
    super(NUM_PHASES, TIMEOUT_SEC, "NetworkMonitorClient_" + CryptoUtils.getRandomString(),
        "Network monitor test");
    gatherStats_ = true;
    clientsAvailabilities_ = new HashMap<CommAddress, Double>();
  }

  @Override
  public void initClients() {
    sleep(INITIAL_SLEEP);
    Iterator<CommAddress> it = clients_.iterator();
    List<CommAddress> clients = new ArrayList<CommAddress>(peersNeeded_);
    for (int i = 0; i < peersNeeded_; ++i)
      clients.add(it.next());
    double availability;
    for (int i = 0; i < peersNeeded_; ++i) {
      availability = random_.nextDouble();
      logger_.info(String.format("Peer %s availability: %.2f", clients.get(i).toString(),
          availability));
      clientsAvailabilities_.put(clients.get(i), availability);
      networkQueue_.add(new InitMessage(clientsJobId_, null, clients.get(i),
          new NetworkMonitorTestClient(jobId_, NUM_PHASES, commAddress_, clients, availability)));
    }
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics statistics) {
    NetworkMonitorStatistics stats = (NetworkMonitorStatistics) statistics;
    for (Pair<CommAddress, Double> record : stats.getEstimatedAvailabilities()) {
      double expectedAvailability = clientsAvailabilities_.get(record.getFirst());
      if (Math.abs(record.getSecond() - expectedAvailability) > ESTIMATION_PRECISION) {
        endWithError(new NebuloException("Availability estimation not correct... " +
            String.format("Is %,2f, expected %,2f", record.getSecond(),
                expectedAvailability)));
      } else {
        logger_
            .info(sender.toString() + " estimates " + record.getFirst().toString() +
          " availability to: " + record.getSecond() + " (expected: " + expectedAvailability +
          ")");
      }

    }
  }

  @Override
  protected String getAdditionalStats() {
    return "";
  }
}
