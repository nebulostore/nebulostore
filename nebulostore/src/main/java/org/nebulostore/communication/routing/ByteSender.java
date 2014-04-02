package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;

/**
 * Sends given bytes to intended recipients.
 *
 * The implementation of this class should send messages of the following format (all numbers are in
 * big-endian format):
 *
 * Version 1 - There are 3 fields: 1. Send 16 bit number indicating the version number (1). 2. Send
 * 32 bit number indicating the length of the message after this field in bytes. 3. Send the message
 * given by user.
 *
 * @author Grzegorz Milka
 */
public interface ByteSender {
  int INT_FIELD_LENGTH = 4;
  int VERSION_FIELD_LENGTH = 2;
  byte[] VERSION_FIELD = {0, 1};
  int VERSION = 1;

  /**
   * {@link #sendMessage(InetSocketAddress, byte[], BlockingQueue, SendOperationIdentifier)}.
   */
  ByteSendFuture sendMessage(InetSocketAddress address, byte[] msg);

  /**
   * Send message over network.
   *
   * @param address
   *          to which to send message
   * @param msg
   * @param results
   *          queue to which results of the operation are sent
   * @param id
   *          identifier of this send operation
   * @return Future which represents the operation
   */
  ByteSendFuture sendMessage(InetSocketAddress address, byte[] msg,
      BlockingQueue<ByteSendResult> results, SendOperationIdentifier id);

  void sendMessageSynchronously(InetSocketAddress address, byte[] msg) throws InterruptedException,
      IOException;

  /**
   * Start this sender.
   */
  void startUp();

  /**
   * Stop and wait for shutdown of all senders.
   *
   * @throws InterruptedException
   */
  void shutDown() throws InterruptedException;
}
