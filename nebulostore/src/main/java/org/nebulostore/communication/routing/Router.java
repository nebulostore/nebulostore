package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.CompletionServiceFactory;

/**
 * This object transfers messages to their destination. Either by sending it to network
 * destination or by listening for messages and directing them to correct listener.
 *
 * For each incoming message every message listener that matches that message receives it.
 *
 * On shutdown no new messages are dispatched.
 *
 * This is a service that should be started before use and stopped when no longer useful.
 *
 * @author Grzegorz Milka
 *
 */
public class Router implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(Router.class);
  private final ListenerService listenerService_;
  private final MessageSender msgSender_;
  private final Map<MessageListener, MessageMatcher> listeners_;
  private final ExecutorService executor_;

  private final CommAddress localCommAddress_;

  private Future<?> thisFuture_;

  @Inject
  public Router(
      ListenerService listener,
      MessageSender sender,
      @Named("communication.local-comm-address") CommAddress commAddress,
      @Named("communication.routing.router-executor") ExecutorService executor) {
    listenerService_ = listener;
    msgSender_ = sender;
    listeners_ = new ConcurrentHashMap<>();
    executor_ = executor;

    localCommAddress_ = commAddress;
  }

  public void addMessageListener(MessageMatcher matcher, MessageListener listener) {
    LOGGER.trace(String.format("addMessageListener(%s, %s)", matcher, listener));
    listeners_.put(listener, matcher);
  }

  public void removeMessageListener(MessageListener listener) {
    LOGGER.trace(String.format("removeMessageListener(%s)", listener));
    listeners_.remove(listener);
  }

  @Override
  public void run() {
    BlockingQueue<CommMessage> queue = listenerService_.getListeningQueue();
    while (true) {
      CommMessage msg;
      try {
        msg = queue.take();
        LOGGER.trace(String.format("Received message: %s", msg.toString()));
      } catch (InterruptedException e) {
        break;
      }
      dispatchMessage(msg);
    }
  }

  /**
   * Send message over network.
   *
   * @see MessageSender
   *
   * @param msg
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg) {
    attachLocalCommAddress(msg);
    return msgSender_.sendMessage(msg);
  }

  /**
   * Send message over network and add result to queue.
   *
   * @see MessageSender
   *
   * @param msg
   * @param resultQueue queue to which add result
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg, BlockingQueue<SendResult> resultQueue) {
    attachLocalCommAddress(msg);
    return msgSender_.sendMessage(msg, resultQueue);
  }

  /**
   * Send message over network using CompletionService returned by {@link CompletionServiceFactory}.
   *
   * @see MessageSender
   *
   * @param msg
   * @param complServiceFactory completion service factory to use.
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg,
      CompletionServiceFactory<CommMessage> complServiceFactory) {
    attachLocalCommAddress(msg);
    return msgSender_.sendMessage(msg, complServiceFactory);
  }

  public void start() throws IOException {
    thisFuture_ = executor_.submit(this);
    listenerService_.start();
  }

  public void shutDown() throws InterruptedException {
    msgSender_.shutDown();
    listenerService_.stop();
    thisFuture_.cancel(true);
  }

  private void attachLocalCommAddress(CommMessage msg) {
    msg.setSourceAddress(localCommAddress_);
  }

  private void dispatchMessage(CommMessage msg) {
    for (Map.Entry<MessageListener, MessageMatcher> entry: listeners_.entrySet()) {
      LOGGER.trace(String.format("Matching %s with %s, result: %s", msg, entry.getValue(),
          entry.getValue().matchMessage(msg)));
      if (entry.getValue().matchMessage(msg)) {
        entry.getKey().onMessageReceive(msg);
      }
    }
  }
}
