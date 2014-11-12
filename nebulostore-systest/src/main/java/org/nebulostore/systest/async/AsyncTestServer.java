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
 * Starts NUM_PEERS_IN_GROUP * NUM_GROUPS peers, divides them into three groups. During every
 * iteration of the test:<br>
 * - first group of peers pretends to be disabled (<b>disabled group</b>)<br>
 * - peers in the second group are synchro-peers of peers from first group (one synchro-peer per
 * "disabled" peer) (<b>synchro-peers group</b>)<br>
 * - peers in the third group send messages to all peers from the first group
 * (<b>senders group</b>)<br>
 * <br>
 * Each iteration consist of several steps:<br>
 * 1. Peers from disabled group disable their communication overlays.<br>
 * 2. Each peer from the senders group sends IncrementMessage to all peers from the disabled
 * group.<br>
 * 3. Peers from the disabled group enable their communication overlays.<br>
 * 4. Peers from the disabled group wait for messages from synchro-peers<br>
 * <br>
 * After each iteration groups change their roles cyclically. One turn consist of three iterations.
 * In one turn each peer belongs to each of group once. At the end of the test the counter
 * module's value of each peer should be equal to NUM_TURNS * NUM_PEERS_IN_GROUP.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestServer extends ConductorServer {

  private static Logger logger_ = Logger.getLogger(AsyncTestServer.class);

  private static final int NUM_GROUPS = 3;
  private static final int NUM_TURNS = 2;
  private static final int NUM_PHASES = NUM_TURNS * 9;
  private static final int NUM_PEERS_IN_GROUP = 2;
  private static final int PEERS_NEEDED = NUM_PEERS_IN_GROUP * NUM_GROUPS;
  private static final int TIMEOUT_SEC = 500;

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
    for (int i = 0; i < NUM_GROUPS; ++i) {
      clients.add(new LinkedList<CommAddress>());
    }

    for (int i = 0; i < peersNeeded_; ++i) {
      clients.get(i % NUM_GROUPS).add(it.next());
    }

    for (int i = 0; i < NUM_GROUPS; ++i) {
      for (int j = 0; j < clients.get(i).size(); ++j) {
        List<CommAddress> neighbor = clients.get((i + 1) % NUM_GROUPS);
        networkQueue_.add(new InitMessage(clientsJobId_, null, clients.get(i).get(j),
            new AsyncTestClient(jobId_, NUM_PHASES, commAddress_, neighbor, neighbor,
                AsyncTestClientState.values()[i])));
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
