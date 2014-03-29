package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import java.io.IOException;
import java.rmi.NoSuchObjectException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Server for message exchange test.
 *
 * This server runs as follows:
 * 1. Set up remote TestController.
 * 2. Wait for required number of test clients or abort if time out. Send
 *    complete client list to each client.
 * 3. Wait for results and process them.
 *
 * This callable returns null if test failed due to small number of clients.
 *
 * @author Grzegorz Milka
 *
 */
public class MessageExchangeTestServer implements Callable<Map<CommAddress, TestResult>> {
  public static final String TEST_CONTROLLER_NAME = "RMI_MESSAGE_EXCHANGE_TEST_CONTROLLER";
  private static final Logger LOGGER = Logger.getLogger(MessageExchangeTestServer.class);

  private final int nOfClients_;
  private final int clientWaitTimeout_;
  private final int answerWaitTimeout_;
  private final Registry registry_;

  private final List<CommAddress> registeredClients_;
  private final TestController testController_;

  private final CountDownLatch testStart_;
  private final CountDownLatch testFinish_;
  private final Map<CommAddress, TestResult> testResults_;

  public MessageExchangeTestServer(
      int nOfClients,
      int clientWaitTimeout,
      int answerWaitTimeout,
      Registry registry) {
    nOfClients_ = nOfClients;
    clientWaitTimeout_ = clientWaitTimeout;
    answerWaitTimeout_ = answerWaitTimeout;
    registry_ = registry;

    registeredClients_ = Collections.synchronizedList(new LinkedList<CommAddress>());
    testController_ = new TestControllerImpl();

    testStart_ = new CountDownLatch(nOfClients_);
    testFinish_ = new CountDownLatch(nOfClients_);
    testResults_ = new ConcurrentHashMap<>(nOfClients_);
  }

  @Override
  public Map<CommAddress, TestResult> call() throws IOException, InterruptedException {
    LOGGER.info(String.format("Running test server for %d clients. Waiting times are:" +
      " (%d, %d).", nOfClients_, clientWaitTimeout_, answerWaitTimeout_));
    startUp();
    waitForResults();
    shutDown();

    if (registeredClients_.size() < nOfClients_) {
      LOGGER.info("Test failed due to small number of clients.");
      return null;
    } else {
      LOGGER.info(String.format("Returning test results of %d hosts: %s", testResults_.size(),
          testResults_.toString()));
      return testResults_;
    }
  }

  private void startUp() throws RemoteException {
    LOGGER.debug("Exporting TestController stub.");
    Remote stub = UnicastRemoteObject.exportObject(testController_, 0);
    registry_.rebind(TEST_CONTROLLER_NAME, stub);
  }

  private void shutDown() {
    try {
      LOGGER.debug("Unexporting TestController stub.");
      UnicastRemoteObject.unexportObject(testController_, false);
    } catch (NoSuchObjectException e) {
      LOGGER.warn("Could not unexport localAddressMap.", e);
    }
  }

  private void waitForResults() throws InterruptedException {
    testStart_.await(clientWaitTimeout_, TimeUnit.MILLISECONDS);
    testFinish_.await(answerWaitTimeout_, TimeUnit.MILLISECONDS);
  }

  /**
   * @author Grzegorz Milka
   */
  private class TestControllerImpl implements TestController {
    @Override
    public Collection<CommAddress> registerClient(CommAddress address) throws RemoteException {
      registeredClients_.add(address);
      LOGGER.info(String.format("New client has registered: %s [%d/%d]", address,
          registeredClients_.size(), nOfClients_));

      try {
        testStart_.countDown();
        testStart_.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException();
      }
      return registeredClients_;
    }

    @Override
    public void sendResult(CommAddress commAddress, TestResult result) {
      testResults_.put(commAddress, result);
      testFinish_.countDown();
      try {
        testFinish_.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException();
      }
    }
  }
}
