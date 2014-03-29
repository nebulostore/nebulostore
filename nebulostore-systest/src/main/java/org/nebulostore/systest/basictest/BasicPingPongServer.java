package org.nebulostore.systest.basictest;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.CaseStatistics;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Sets up Basic PingPong test.
 * @author szymonmatejczyk
 */
public final class BasicPingPongServer extends ConductorServer {
  private static Logger logger_ = Logger.getLogger(BasicPingPongServer.class);
  private static final int NUM_PHASES = 2;
  private static final int NUM_CLIENTS = 2;
  private static final int INITIAL_SLEEP = 2000;
  private static final int TIMEOUT_SEC = 60;

  public BasicPingPongServer() {
    super(NUM_PHASES, NUM_CLIENTS, TIMEOUT_SEC, "PingPongClient_" + CryptoUtils.getRandomString(),
        "BasicPingPong server");
  }

  @Override
  public void initClients() {
    sleep(INITIAL_SLEEP);
    Iterator<CommAddress> i = clients_.iterator();
    CommAddress pingAddress = i.next();
    logger_.debug("Initializing ping: " + pingAddress.toString());
    CommAddress pongAddress = i.next();
    logger_.debug("Initializing pong: " + pongAddress.toString());

    networkQueue_.add(new InitMessage(clientsJobId_, null, pingAddress,
        new PingClient(jobId_, commAddress_, NUM_PHASES, pongAddress)));
    networkQueue_.add(new InitMessage(clientsJobId_, null, pongAddress,
        new PongClient(jobId_, commAddress_, NUM_PHASES)));
  }

  @Override
  public void feedStats(CommAddress sender, CaseStatistics stats) {
  }

  @Override
  protected String getAdditionalStats() {
    return "";
  }
}
