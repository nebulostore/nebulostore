package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.naming.CommAddressResolver;

public class MessageSenderAdapter implements MessageSender {
  private static final Logger LOGGER = Logger.getLogger(MessageSenderAdapter.class);
  private final ByteSender byteSender_;
  private final ExecutorService executor_;
  private final CommAddressResolver resolver_;

  @Inject
  public MessageSenderAdapter(ByteSender byteSender,
      @Named("communication.routing.sender-worker-executor") ExecutorService executor,
      CommAddressResolver resolver) {
    byteSender_ = byteSender;
    executor_ = executor;
    resolver_ = resolver;
  }

  /**
   * Send message over network.
   *
   * @param msg
   * @return Future which on failure may throw {@link AddressNotPresentException},
   *         {@code InterruptedException} and {@code IOException}.
   */
  @Override
  public MessageSendFuture sendMessage(CommMessage msg) {
    return sendMessage(msg, null);
  }

  /**
   * Send message over network and add results to queue.
   *
   * @param msg
   * @param results
   *          queue to which send result of the operation
   * @param id
   *          id by which client can identify the operation
   * @return {@link MessageSendFuture}
   */
  @Override
  public MessageSendFuture sendMessage(CommMessage msg, BlockingQueue<SendResult> results) {
    LOGGER.debug("sendMessage(" + msg + ")");
    MessageSenderCallable sendTask = new MessageSenderCallable(msg, results);
    executor_.submit(sendTask);
    return sendTask;
  }

  @Override
  public void sendMessageSynchronously(CommMessage msg) throws AddressNotPresentException,
      InterruptedException, IOException {
    LOGGER.debug("sendMessageSynchronously(" + msg + ")");
    MessageSenderCallable sendTask = new MessageSenderCallable(msg);
    SendResult result = sendTask.call();
    result.getResult();
  }

  public void startUp() {
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
  private class MessageSenderCallable implements Callable<SendResult>, MessageSendFuture {
    private final CommMessage commMsg_;
    private final BlockingQueue<SendResult> resultQueue_;
    private SendResult result_;

    private final AtomicBoolean isCancelled_;
    private final AtomicBoolean isDone_;

    public MessageSenderCallable(CommMessage msg) {
      this(msg, null);
    }

    public MessageSenderCallable(CommMessage msg, BlockingQueue<SendResult> resultQueue) {
      commMsg_ = msg;
      resultQueue_ = resultQueue;

      isCancelled_ = new AtomicBoolean(false);
      isDone_ = new AtomicBoolean(false);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      synchronized (this) {
        if (isDone_.get()) {
          return false;
        } else {
          isCancelled_.set(true);
        }
      }
      return false;
    }

    @Override
    public boolean get() throws AddressNotPresentException, InterruptedException, IOException {
      while (!isDone_.get()) {
        synchronized (this) {
          wait();
        }
      }
      return result_.getResult();
    }

    @Override
    public boolean get(long timeout, TimeUnit unit) throws AddressNotPresentException,
        InterruptedException, IOException, TimeoutException {
      if (!isDone_.get()) {
        synchronized (this) {
          unit.timedWait(this, timeout);
        }
      }
      if (!isDone_.get()) {
        throw new TimeoutException();
      }
      return result_.getResult();
    }

    @Override
    public boolean isCancelled() {
      return isDone() && isCancelled_.get();
    }

    @Override
    public boolean isDone() {
      return isDone_.get();
    }

    @Override
    public SendResult call() {
      LOGGER.debug("MessageSenderCallable.call() with CommMessage: " + commMsg_);
      if (isCancelled_.get()) {
        result_ = new SendResult(commMsg_, new CancellationException());
        synchronized (this) {
          isDone_.set(true);
          this.notifyAll();
          return result_;
        }
      }
      InetSocketAddress destAddress = null;
      try {
        destAddress = resolver_.resolve(commMsg_.getDestinationAddress());
      } catch (AddressNotPresentException e) {
        result_ = new SendResult(commMsg_, e);
      } catch (IOException e) {
        result_ = new SendResult(commMsg_, e);
      }

      byte[] msg = serialize(commMsg_);
      try {
        if (destAddress != null) {
          byteSender_.sendMessageSynchronously(destAddress, msg);
          result_ = new SendResult(commMsg_);
        }
      } catch (InterruptedException e) {
        result_ = new SendResult(commMsg_, e);
      } catch (IOException e) {
        result_ = new SendResult(commMsg_, e);
        resolver_.reportFailure(commMsg_.getDestinationAddress());
      } finally {
        if (resultQueue_ != null) {
          resultQueue_.add(result_);
        }
      }
      synchronized (this) {
        isDone_.set(true);
        this.notifyAll();
      }
      return result_;
    }

    private byte[] serialize(CommMessage commMsg) {
      return SerializationUtils.serialize(commMsg);
    }
  }
}
