package org.nebulostore.communication.routing;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.naming.CommAddressResolver;
import org.nebulostore.communication.routing.SendResult.ResultType;
import org.nebulostore.utils.CompletionServiceFactory;
import org.nebulostore.utils.ContextedException;

/**
 * Sends given messages to intended recipients.
 *
 * @author Grzegorz Milka
 */
public class MessageSender {
  private static final Logger LOGGER = Logger.getLogger(MessageSender.class);
  private final ExecutorService executor_;
  private final OOSDispatcher dispatcher_;
  private final CommAddressResolver resolver_;

  @Inject
  public MessageSender(
      @Named("communication.routing.sender-worker-executor") ExecutorService executor,
      OOSDispatcher dispatcher,
      CommAddressResolver resolver) {
    executor_ = executor;
    dispatcher_ = dispatcher;
    resolver_ = resolver;
  }

  /**
   * @return Executor used for running message sending tasks.
   */
  public Executor getSenderExecutor() {
    return executor_;
  }

  /**
   * Send message over network.
   *
   * @param msg
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg) {
    LOGGER.debug("sendMessage(" + msg + ")");
    return executor_.submit(new MessageSenderCallable(msg));
  }

  /**
   * Send message over network and add results to queue.
   *
   * @param msg
   * @param results queue to which send result of the operation.
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg, BlockingQueue<SendResult> results) {
    LOGGER.debug("sendMessage(" + msg + ")");
    return executor_.submit(new MessageSenderCallable(msg, results));
  }

  /**
   * Send message using CompletionService produced by given factory.
   *
   * @param msg
   * @param complServiceFactory
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *  {@code InterruptedException} and {@code IOException}.
   */
  public Future<CommMessage> sendMessage(CommMessage msg,
      CompletionServiceFactory<CommMessage> complServiceFactory) {
    LOGGER.debug("sendMessage(" + msg + ")");
    return complServiceFactory.getCompletionService(executor_).submit(
        new MessageSenderCallable(msg));
  }

  /**
   * Stop and wait for shutdown of all senders.
   *
   * @throws InterruptedException
   */
  public void shutDown() throws InterruptedException {
    executor_.shutdown();
    executor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    LOGGER.debug("shutDown(): void");
  }

  /**
   * Simple runnable which handles sending CommMessage over network.
   */
  private class MessageSenderCallable implements Callable<CommMessage> {
    private final CommMessage commMsg_;
    private final BlockingQueue<SendResult> resultQueue_;

    public MessageSenderCallable(CommMessage msg) {
      commMsg_ = msg;
      resultQueue_ = null;
    }

    public MessageSenderCallable(CommMessage msg, BlockingQueue<SendResult> resultQueue) {
      commMsg_ = msg;
      resultQueue_ = resultQueue;
    }

    @Override
    public CommMessage call() throws ContextedException {
      LOGGER.debug("MessageSenderCallable.call() with CommMessage: " + commMsg_);
      InetSocketAddress destAddress;
      try {
        destAddress = resolver_.resolve(commMsg_.getDestinationAddress());
      } catch (IOException e) {
        throw new ContextedException(e, commMsg_);
      } catch (AddressNotPresentException e) {
        throw new ContextedException(e, commMsg_);
      }

      boolean isSuccess = false;
      ObjectOutputStream oos = null;
      try {
        oos = dispatcher_.getStream(destAddress);
        oos.writeObject(commMsg_);
        oos.flush();
        LOGGER.trace("MessageSender has successfully sent message: " + commMsg_);
        isSuccess = true;
      } catch (IOException e) {
        LOGGER.warn(String.format("sendMessage(%s) -> error", commMsg_), e);
        resolver_.reportFailure(commMsg_.getDestinationAddress());
        throw new ContextedException(e, commMsg_);
      } catch (InterruptedException e) {
        throw new ContextedException(e, commMsg_);
      } finally {
        if (resultQueue_ != null) {
          if (isSuccess) {
            resultQueue_.add(new SendResult(ResultType.OK, commMsg_));
          } else {
            resultQueue_.add(new SendResult(ResultType.ERROR, commMsg_));
          }
        }
        if (oos != null) {
          dispatcher_.putStream(destAddress);
        }
      }
      return commMsg_;
    }
  }
}
