package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.CommunicationFacade;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.systest.newcommunication.functionaltest.messageexchange.PingPongMessage.Type;
import org.nebulostore.utils.CompletionServiceReader;
import org.nebulostore.utils.ContextedException;
import org.nebulostore.utils.SingleCompletionServiceFactory;

/**
 * Client for message exchange test.
 *
 * This client runs as follows:
 * 1. Find and get remote TestController interface
 * 2. Start up CommunicationFacade
 * 3. Get from TestController CommAddresses which should ping and ping them.
 * 4. Respond with pong messages to every ping message
 * 5. Once it receives all pong messages or times out,
 *    send test results to test controller and shut down.
 *
 * @author Grzegorz Milka
 */
public class MessageExchangeTestClient implements Callable<TestResult> {
  private static final Logger LOGGER = Logger.getLogger(MessageExchangeTestClient.class);

  private final Registry registry_;
  private final int waitTimeOut_;
  private final CommAddress localCommAddress_;
  private final CommunicationFacade commFacade_;

  private final SingleCompletionServiceFactory<CommMessage> msgSendComplServiceFactory_;
  private final CompletionServiceReader<CommMessage> msgSendComplServiceReader_;

  private final Collection<CommAddress> failedPings_;
  private final Collection<CommAddress> receivedPongs_;

  private final Collection<CommAddress> expectedPongs_;

  private final PongListener pongListener_;
  private final PongResponder pongResponder_;

  private TestController testController_;

  public MessageExchangeTestClient(
      Registry registry,
      int waitTimeOut,
      CommAddress localCommAddress,
      CommunicationFacade commFacade) {
    registry_ = registry;
    waitTimeOut_ = waitTimeOut;
    localCommAddress_ = localCommAddress;
    commFacade_ = commFacade;

    msgSendComplServiceFactory_ = new SingleCompletionServiceFactory<>();
    msgSendComplServiceReader_ = msgSendComplServiceFactory_.getCompletionServiceReader();

    failedPings_ = new LinkedList<CommAddress>();
    receivedPongs_ = Collections.synchronizedList(new LinkedList<CommAddress>());
    expectedPongs_ = new LinkedList<CommAddress>();

    pongListener_ = new PongListener();
    pongResponder_ = new PongResponder();
  }

  @Override
  public TestResult call() throws NotBoundException, IOException, InterruptedException {
    try {
      startUp();
      runTest();
      shutDown();
    } catch (NotBoundException | IOException | InterruptedException e) {
      throw e;
    }

    return new TestResult(failedPings_, receivedPongs_);
  }

  private void aggregateSendConfirmations(Collection<CommAddress> otherClients)
      throws InterruptedException {
    Collection<CommAddress> sentPings = new LinkedList<>();
    for (int i = 0; i < otherClients.size(); ++i) {
      Future<CommMessage> future = msgSendComplServiceReader_.take();
      try {
        CommMessage msg = future.get();
        LOGGER.debug(String.format("Message to: %s has been sent successfully.",
            msg.getDestinationAddress().toString()));
        expectedPongs_.add(msg.getDestinationAddress());
        sentPings.add(msg.getDestinationAddress());
      } catch (ExecutionException e) {
        ContextedException contextException = (ContextedException) e.getCause();
        CommMessage commMsg = (CommMessage) contextException.getContext();
        LOGGER.debug(String.format("Could not send ping to: %s.", commMsg.getDestinationAddress()),
            e);
      }
    }
    failedPings_.addAll(otherClients);
    failedPings_.removeAll(sentPings);
  }

  private void runTest() throws InterruptedException, RemoteException {
    LOGGER.info("runTest -> registerClient");
    Collection<CommAddress> otherClients = testController_.registerClient(localCommAddress_);

    LOGGER.info("runTest -> send messages");
    for (CommAddress recipient: otherClients) {
      commFacade_.sendMessage(new PingPongMessage(localCommAddress_, recipient, Type.PING),
          msgSendComplServiceFactory_);
    }

    LOGGER.info("runTest -> aggregateSendConfirmations");
    aggregateSendConfirmations(otherClients);

    LOGGER.info("runTest -> waitForPongs");
    waitForPongs();

    LOGGER.trace(String.format("Sending result with %d failedPings and %d receivedPongs.",
        failedPings_.size(), receivedPongs_.size()));
    testController_.sendResult(localCommAddress_, new TestResult(failedPings_, receivedPongs_));
  }

  private void startUp() throws NotBoundException, IOException {
    testController_ = (TestController) registry_.lookup(
        MessageExchangeTestServer.TEST_CONTROLLER_NAME);
    startUpCommunicationFacade();
  }

  private void shutDown() throws InterruptedException {
    testController_ = null;
    shutDownCommunicationFacade();
  }

  private void startUpCommunicationFacade() throws IOException {
    commFacade_.addMessageListener(PingPongMessage.getMessageMatcher(Type.PONG), pongListener_);
    commFacade_.addMessageListener(PingPongMessage.getMessageMatcher(Type.PING), pongResponder_);
    commFacade_.startUp();
  }

  private void shutDownCommunicationFacade() throws InterruptedException {
    commFacade_.shutDown();
    commFacade_.removeMessageListener(pongListener_);
    commFacade_.removeMessageListener(pongResponder_);
  }

  private void waitForPongs() throws InterruptedException {
    synchronized (expectedPongs_) {
      while (expectedPongs_.size() > 0) {
        expectedPongs_.wait(waitTimeOut_);
      }
    }
  }

  /**
   * MessageListener which records pong messages.
   *
   * @author Grzegorz Milka
   *
   */
  private class PongListener implements MessageListener {
    @Override
    public void onMessageReceive(Message msg) {
      receivedPongs_.add(((CommMessage) msg).getSourceAddress());
      LOGGER.debug("Received pong message from: " + ((CommMessage) msg).getSourceAddress());

      synchronized (expectedPongs_) {
        expectedPongs_.remove(((CommMessage) msg).getSourceAddress());
        if (expectedPongs_.size() == 0) {
          expectedPongs_.notify();
        }
      }
    }
  }

  /**
   * MessageListener which sends pong on received message.
   *
   * @author Grzegorz Milka
   *
   */
  private class PongResponder implements MessageListener {
    @Override
    public void onMessageReceive(Message msg) {
      CommMessage commMsg = (CommMessage) msg;
      LOGGER.debug("Received ping message from: " + commMsg.getSourceAddress());
      commFacade_.sendMessage(new PingPongMessage(
          localCommAddress_, commMsg.getSourceAddress(), Type.PONG));
    }
  }


}
