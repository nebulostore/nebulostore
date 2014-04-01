package org.nebulostore.communication.routing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.utils.CompletionServiceFactory;

/**
 * Sends given messages to intended recipients.
 *
 * @author Grzegorz Milka
 */
public interface MessageSender {
  /**
   * Send message over network.
   *
   * @param msg
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  Future<CommMessage> sendMessage(CommMessage msg);

  /**
   * Send message over network and add results to queue.
   *
   * @param msg
   * @param results queue to which send result of the operation.
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  Future<CommMessage> sendMessage(CommMessage msg, BlockingQueue<SendResult> results);

  /**
   * Send message using CompletionService produced by given factory.
   *
   * @param msg
   * @param complServiceFactory
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  Future<CommMessage> sendMessage(CommMessage msg,
      CompletionServiceFactory<CommMessage> complServiceFactory);

  /**
   * Stop and wait for shutdown of all senders.
   *
   * @throws InterruptedException
   */
  void shutDown() throws InterruptedException;
}
