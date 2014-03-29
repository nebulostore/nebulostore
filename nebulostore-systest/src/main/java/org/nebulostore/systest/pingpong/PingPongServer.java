package org.nebulostore.systest.pingpong;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Sets up PingPong test.
 * @author szymonmatejczyk, lukaszsiczek
 */
public final class PingPongServer extends ConductorServer {
  private static Logger logger_ = Logger.getLogger(PingPongServer.class);
  private static final int NUM_PHASES = 6;
  private static final int NUM_CLIENTS = 15;
  private static final int INITIAL_SLEEP = 2000;
  private static final int TIMEOUT_SEC = 240;

  public PingPongServer() {
    super(NUM_PHASES, NUM_CLIENTS, TIMEOUT_SEC, "PingPongClient_" + CryptoUtils.getRandomString(),
        "PingPong server");
  }

  @Override
  public void initClients() {
    sleep(INITIAL_SLEEP);
    CommAddress[] clients = clients_.toArray(new CommAddress[NUM_CLIENTS]);
    for (int i = 0; i < clients.length; ++i) {
      logger_.debug("Initializing ping-pong client: " + clients[i].toString());
      List<CommAddress> children = new LinkedList<CommAddress>();
      if (2 * i + 2 < NUM_CLIENTS) {
        children.add(clients[2 * i + 1]);
        children.add(clients[2 * i + 2]);
      }
      networkQueue_.add(new InitMessage(clientsJobId_, null, clients[i],
                new PingPongClient(jobId_, commAddress_, NUM_PHASES, children, i)));
    }
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics stats) {
  }

  @Override
  protected String getAdditionalStats() {
    return "";
  }
}
