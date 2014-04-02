package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;

/**
 * Sends given messages to intended recipients.
 *
 * @author Grzegorz Milka
 */
public interface MessageSender {
  /**
   * {@link #sendMessage(CommMessage, BlockingQueue, SendOperationIdentifier)}.
   */
  MessageSendFuture sendMessage(CommMessage msg);

  /**
   * Send message over network and add results to queue.
   *
   * @param msg
   * @param results
   *          queue to which send result of the operation.
   * @return {@link MessageSendFuture}
   */
  MessageSendFuture sendMessage(CommMessage msg, BlockingQueue<SendResult> results);

  void sendMessageSynchronously(CommMessage msg) throws AddressNotPresentException,
      InterruptedException, IOException;

  void startUp();

  /**
   * Stop and wait for shutdown of all senders.
   *
   * @throws InterruptedException
   */
  void shutDown() throws InterruptedException;
}
