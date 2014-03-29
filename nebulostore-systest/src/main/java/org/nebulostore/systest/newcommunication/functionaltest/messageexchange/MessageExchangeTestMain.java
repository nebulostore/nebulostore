package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.nebulostore.communication.CommunicationFacade;
import org.nebulostore.communication.CommunicationFacadeConfiguration;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Grzegorz Milka
 */
public final class MessageExchangeTestMain {
  private static final Logger LOGGER = Logger.getLogger(MessageExchangeTestMain.class);
  private static final String CONFIGURATION_PATH = "resources/conf/Peer.xml";

  private static XMLConfiguration xmlConfig_;

  private final boolean isServer_;
  private final int clientCount_;
  private final int clientTimeout_;
  private final int answerTimeout_;

  private final String serverAddress_;
  private final int contactTimeout_;

  private MessageExchangeTestMain(
      int clientCount,
      int clientTimeout,
      int answerTimeout) {
    isServer_ = true;
    clientCount_ = clientCount;
    clientTimeout_ = clientTimeout;
    answerTimeout_ = answerTimeout;

    serverAddress_ = "";
    contactTimeout_ = 0;
  }

  private MessageExchangeTestMain(
      String serverAddress,
      int contactTimeout) {
    isServer_ = false;
    serverAddress_ = serverAddress;
    contactTimeout_ = contactTimeout;

    clientCount_ = 0;
    clientTimeout_ = 0;
    answerTimeout_ = 0;
  }

  /**
   * Main.
   *
   * @param args
   * @throws RemoteException
   * @throws ConfigurationException
   */
  public static void main(final String[] args) throws RemoteException, ConfigurationException {
    xmlConfig_ = new XMLConfiguration(CONFIGURATION_PATH);

    boolean isServer =
          xmlConfig_.getString("systest.communication.messageexchange.mode").equals("server");
    MessageExchangeTestMain testInstance;

    if (isServer) {
      int clientCount =
          xmlConfig_.getInt("systest.communication.messageexchange.server.client-count");
      int clientTimeout =
          xmlConfig_.getInt("systest.communication.messageexchange.server.client-timeout");
      int answerTimeout =
          xmlConfig_.getInt("systest.communication.messageexchange.server.answer-timeout");
      testInstance = new MessageExchangeTestMain(clientCount, clientTimeout, answerTimeout);
    } else {
      String serverAddress =
          xmlConfig_.getString("systest.communication.messageexchange.client.server-net-address");
      int contactTimeout =
          xmlConfig_.getInt("systest.communication.messageexchange.client.contact-timeout");
      testInstance = new MessageExchangeTestMain(serverAddress, contactTimeout);
    }

    System.exit(testInstance.run());
  }

  private int run() throws RemoteException {
    if (isServer_) {
      return runServer();
    } else {
      return runClient();
    }
  }

  private int runServer() throws RemoteException {
    LOGGER.info(String.format("Starting main test server."));
    try {
      LocateRegistry.createRegistry(1099);
    } catch (RemoteException e) {
      LOGGER.warn("Could not create local registry.", e);
    }

    Registry registry = LocateRegistry.getRegistry();
    MessageExchangeTestServer testServer = new MessageExchangeTestServer(clientCount_,
        clientTimeout_, answerTimeout_, registry);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Map<CommAddress, TestResult>> future = executor.submit(testServer);

    Map<CommAddress, TestResult> results;
    try {
      results = future.get();
    } catch (InterruptedException e) {
      throw new IllegalStateException("Unexpected interrupt.");
    } catch (ExecutionException e) {
      LOGGER.error("Error happened in test server", e);
      return 1;
    }

    if (results == null) {
      return 1;
    }

    int failedCount = 0;
    Map<CommAddress, TestResult> sortedResults = new TreeMap<>(results);
    for (Entry<CommAddress, TestResult> entry: sortedResults.entrySet()) {
      LOGGER.info(String.format(
          "Host: %s has failed to send %d pings, has received %d pongs.",
          entry.getKey(), entry.getValue().getFailedPings().size(),
          entry.getValue().getReceivedPongs().size()));
      if (entry.getValue().getFailedPings().size() > 0 ||
          entry.getValue().getReceivedPongs().size() < results.size()) {
        failedCount += 1;
      }
    }
    failedCount += clientCount_ - results.size();

    if (failedCount > 0) {
      LOGGER.info(String.format("%d hosts have failed.", failedCount));
      return 1;
    } else {
      LOGGER.info(String.format("Test was successful."));
      return 0;
    }
  }

  private int runClient() throws RemoteException {
    Registry registry = LocateRegistry.getRegistry(serverAddress_);

    CommunicationFacadeConfiguration commConfig = new CommunicationFacadeConfiguration(xmlConfig_);
    Injector injector = Guice.createInjector(commConfig);
    CommunicationFacade commFacade = injector.getInstance(CommunicationFacade.class);

    MessageExchangeTestClient testClient = new MessageExchangeTestClient(registry,
        contactTimeout_, injector.getInstance(Key.get(
            CommAddress.class, Names.named("communication.local-comm-address"))),
        commFacade);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<TestResult> future = executor.submit(testClient);

    TestResult testResult;
    try {
      testResult = future.get();
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    } catch (ExecutionException e) {
      LOGGER.error("Error happened in test client.", e);
      return 1;
    }

    LOGGER.info(String.format("%d hosts have responded.", testResult.getReceivedPongs().size()));
    return 0;
  }
}
