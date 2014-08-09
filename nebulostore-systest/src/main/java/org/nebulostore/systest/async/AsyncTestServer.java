package org.nebulostore.systest.async;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.systest.async.AsyncTestClient.AsyncTestClientState;

/**
 * Starts NUM_PEERS_IN_GROUP * 3 peers, divides them into three group. During every iteration of the
 * test:<br>
 * - one group of peers pretends to be disabled<br>
 * - peers in the second group are synchro-peers of peers from first group (one synchro-peer
 *  per "disabled" peer)<br>
 * - peers in the third group send messages to all peers from the first group
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestServer extends ConductorServer {

  private static Logger logger_ = Logger.getLogger(AsyncTestServer.class);

  private static final int NUM_TURNS = 1;
  private static final int NUM_PHASES = NUM_TURNS * 9;
  private static final int NUM_PEERS_IN_GROUP = 1;
  private static final int PEERS_NEEDED = NUM_PEERS_IN_GROUP * 3;
  private static final int TIMEOUT_SEC = 300;

  public AsyncTestServer() {
    super(NUM_PHASES + 1, TIMEOUT_SEC, "AsyncTestServer" + CryptoUtils.getRandomString(),
        "Asynchronous messages test");
    gatherStats_ = true;
    peersNeeded_ = PEERS_NEEDED;
  }

  @Override
  public void initClients() {
    Iterator<CommAddress> it = clients_.iterator();
    List<List<CommAddress>> clients = new LinkedList<List<CommAddress>>();
    for (int i = 0; i < 3; ++i) {
      clients.add(new LinkedList<CommAddress>());
    }

    for (int i = 0; i < peersNeeded_; ++i) {
      clients.get(i % 3).add(it.next());
    }

    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < clients.get(i).size(); ++j) {
        List<CommAddress> neighbor = clients.get((i + 1) % 3);
        networkQueue_.add(new InitMessage(clientsJobId_, null, clients.get(i).get(j),
            new AsyncTestClient(jobId_, NUM_PHASES, commAddress_, neighbor, neighbor.get(j %
                neighbor.size()), AsyncTestClientState.values()[i])));
      }
    }
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics statistics) {
    logger_.info("Received stats from " + sender);
    AsyncTestStatistics stats = (AsyncTestStatistics) statistics;
    if (stats.getCounter() != NUM_TURNS * NUM_PEERS_IN_GROUP) {
      endWithError(new NebuloException("Peer " + sender + " received " + stats.getCounter() +
          " messages, but " + NUM_TURNS * NUM_PEERS_IN_GROUP + " were expected"));
    }
  }

  @Override
  protected String getAdditionalStats() {
    return null;
  }
}
