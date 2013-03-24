package org.nebulostore.systest.lists;

import java.util.Iterator;

import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.communication.address.CommAddress;
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
  private static final int NUM_PHASES = 3;
  private static final int TIMEOUT_SEC = 200;

  public ListsServer() {
    super(NUM_PHASES, TIMEOUT_SEC, "ListsClient_" + CryptoUtils.getRandomString(),
        "Lists server");
  }

  public void initialize(XMLConfiguration config) {
    schedulePhaseTimer();
    initializeFromConfig(config);
    // Do not use server as one of test participants.
    --peersNeeded_;
    useServerAsClient_ = false;
    gatherStats_ = false;
  }

  @Override
  public void initClients() {
    Iterator<CommAddress> it = clients_.iterator();
    CommAddress[] clients = new CommAddress[peersNeeded_];
    for (int i = 0; i < peersNeeded_; ++i)
      clients[i] = it.next();
    for (int i = 0; i < peersNeeded_; ++i)
      networkQueue_.add(new InitMessage(clientsJobId_, null, clients[i],
          new ListsClient(jobId_, NUM_PHASES, clients, i)));
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics stats) { }

  @Override
  protected String getAdditionalStats() {
    return "";
  }
}
