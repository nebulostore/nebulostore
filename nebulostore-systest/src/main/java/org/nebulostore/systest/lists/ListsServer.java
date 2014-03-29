package org.nebulostore.systest.lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Lists test.
 *
 * @author Bolek Kulbabinski
 */
public final class ListsServer extends ConductorServer {
  private static Logger logger_ = Logger.getLogger(ListsServer.class);

  private static final int NUM_PHASES = 3;
  private static final int TIMEOUT_SEC = 430;
  private static final int INITIAL_SLEEP = 8000;
  private static final int PHASE_TIMEOUT = 80;

  public ListsServer() {
    super(NUM_PHASES, TIMEOUT_SEC, "ListsClient_" + CryptoUtils.getRandomString(),
        "Lists server");
    gatherStats_ = true;
    phaseTimeout_ = PHASE_TIMEOUT;
  }

  @Override
  public void initClients() {
    sleep(INITIAL_SLEEP);
    Iterator<CommAddress> it = clients_.iterator();
    List<CommAddress> clients = new ArrayList<CommAddress>(peersNeeded_);
    for (int i = 0; i < peersNeeded_; ++i) {
      clients.add(it.next());
    }
    for (int i = 0; i < peersNeeded_; ++i) {
      networkQueue_.add(new InitMessage(clientsJobId_, null, clients.get(i),
          new ListsClient(jobId_, commAddress_, NUM_PHASES, clients, i)));
    }
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics stats) {
    ListsStats lstStats = (ListsStats) stats;
    StringBuffer result = new StringBuffer();
    List<NebuloAddress> addresses = lstStats.getAddresses();
    for (NebuloAddress addr : addresses) {
      result.append(" " + addr + ",");
    }
    logger_.debug("Attempted " + lstStats.getNTriedFiles() + " downloads. " +
      "Found " + addresses.size() + " unavailable files:" + result);
  }

  @Override
  protected String getAdditionalStats() {
    return "";
  }
}
