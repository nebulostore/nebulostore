package org.nebulostore.communication.routing.plainsocket;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.routing.ByteSendFuture;
import org.nebulostore.communication.routing.ByteSendResult;
import org.nebulostore.communication.routing.ByteSender;
import org.nebulostore.communication.routing.SendOperationIdentifier;

public class PlainSocketByteSender implements ByteSender {
  private static final Logger LOGGER = Logger.getLogger(PlainSocketByteSender.class);
  private final ExecutorService executor_;
  private final AtomicBoolean isShutdown_;
  private final ReadLock readLock_;
  private final WriteLock writeLock_;

  /**
   * @param executor
   *          - owned executor which allows this sender to call shutdown to kill all workers.
   */
  @Inject
  public PlainSocketByteSender(
      @Named("communication.routing.byte-sender-worker-executor") ExecutorService executor) {
    executor_ = executor;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock_ = lock.readLock();
    writeLock_ = lock.writeLock();
    isShutdown_ = new AtomicBoolean(false);
  }

  @Override
  public ByteSendFuture sendMessage(InetSocketAddress dest, byte[] msg) {
    return sendMessage(dest, msg, null, new SendOperationIdentifier(BigInteger.ZERO));
  }

  @Override
  public ByteSendFuture sendMessage(InetSocketAddress dest, byte[] msg,
      BlockingQueue<ByteSendResult> results, SendOperationIdentifier id) {
    if (isShutdown_.get()) {
      throw new IllegalStateException("PlainSocketByteSender is shutdown.");
    }
    ByteSenderCallable sendTask = new ByteSenderCallable(dest, msg, id);
    executor_.submit(sendTask);
    return sendTask;
  }

  @Override
  public void sendMessageSynchronously(InetSocketAddress dest, byte[] msg)
      throws InterruptedException, IOException {
    readLock_.lock();
    try {
      if (isShutdown_.get()) {
        throw new IllegalStateException("PlainSocketByteSender is shutdown.");
      }
      ByteSenderCallable sendTask = new ByteSenderCallable(dest, msg, new SendOperationIdentifier(
          BigInteger.ZERO));
      ByteSendResult result = sendTask.call();
      result.getResult();
    } finally {
      readLock_.unlock();
    }
  }

  @Override
  public void startUp() {
  }

  @Override
  public void shutDown() throws InterruptedException {
    isShutdown_.set(true);
    writeLock_.lock();
    try {
      executor_.shutdown();
      executor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } finally {
      writeLock_.unlock();
    }
  }

  /**
   * Simple runnable which handles sending CommMessage over network.
   */
  private class ByteSenderCallable implements ByteSendFuture, Callable<ByteSendResult> {
    private final InetSocketAddress dest_;
    private final byte[] msg_;
    private final BlockingQueue<ByteSendResult> resultQueue_;
    private final SendOperationIdentifier id_;
    private final AtomicBoolean isRunning_;
    private final AtomicBoolean isCancelled_;
    private final AtomicBoolean isDone_;

    private ByteSendResult result_;
    private Socket socket_;

    public ByteSenderCallable(InetSocketAddress dest, byte[] msg, SendOperationIdentifier id) {
      this(dest, msg, null, id);
    }

    public ByteSenderCallable(InetSocketAddress dest, byte[] msg,
        BlockingQueue<ByteSendResult> resultQueue, SendOperationIdentifier id) {
      dest_ = dest;
      msg_ = msg;
      id_ = id;
      resultQueue_ = resultQueue;
      isRunning_ = new AtomicBoolean(false);
      isCancelled_ = new AtomicBoolean(false);
      isDone_ = new AtomicBoolean(false);
    }

    @Override
    public ByteSendResult call() {
      LOGGER.debug("ByteSenderRunnable.run() to destination: " + dest_);
      try {
        synchronized (this) {
          if (isCancelled_.get()) {
            result_ = new ByteSendResult(id_, new CancellationException());
          }
          socket_ = createSocket(dest_);
        }
      } catch (IOException e) {
        result_ = new ByteSendResult(id_, e);
      }
      if (socket_ == null) {
        finish(result_);
        return result_;
      }
      try {
        OutputStream os = socket_.getOutputStream();
        os.write(ByteSender.VERSION_FIELD);
        byte[] lenArray = getLengthByteArray(msg_.length);
        os.write(lenArray);
        os.write(msg_);
        os.close();
      } catch (IOException e) {
        if (isCancelled_.get()) {
          result_ = new ByteSendResult(id_, new CancellationException());
        } else {
          result_ = new ByteSendResult(id_, e);
        }
      } finally {
        synchronized (this) {
          if (!socket_.isClosed()) {
            try {
              socket_.close();
            } catch (IOException e) {
              LOGGER.debug("ByteSenderRunnable.run() -> IOException when closing socket.", e);
            }
          }
        }
      }
      if (result_ == null) {
        result_ = new ByteSendResult(id_);
      }
      finish(result_);
      return result_;
    }

    @Override
    public synchronized boolean cancel(boolean mayInterruptIfRunning) {
      if (isDone()) {
        return false;
      } else {
        isCancelled_.set(true);
        if (mayInterruptIfRunning && isRunning_.get()) {
          if (socket_ != null) {
            try {
              socket_.close();
            } catch (IOException e) {
              LOGGER.debug("ByteSenderRunnable.cancel() -> IOException when closing socket.", e);
            }
          }
        }
        return true;
      }
    }

    @Override
    public SendOperationIdentifier getId() {
      return id_;
    }

    @Override
    public synchronized boolean get() throws InterruptedException, IOException {
      while (result_ == null) {
        this.wait();
      }
      return result_.getResult();
    }

    @Override
    public synchronized boolean get(long timeout, TimeUnit unit) throws InterruptedException,
        IOException, TimeoutException {
      if (result_ == null) {
        unit.timedWait(this, timeout);
      }
      if (result_ == null) {
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

    private void finish(ByteSendResult result) {
      if (resultQueue_ != null) {
        try {
          resultQueue_.put(result);
        } catch (InterruptedException e) {
          throw new IllegalStateException("Unexpected interrupt.", e);
        }
      }
      synchronized (this) {
        this.notifyAll();
      }
    }

    private byte[] getLengthByteArray(int length) {
      byte[] intArray = new byte[ByteSender.INT_FIELD_LENGTH];
      ByteBuffer buf = ByteBuffer.wrap(intArray);
      buf.order(ByteOrder.BIG_ENDIAN);
      buf.putInt(msg_.length);
      return buf.array();
    }
  }

  /**
   * Creates socket to host pointed by commAddress.
   */
  private Socket createSocket(InetSocketAddress dest) throws IOException {
    Socket socket = null;
    try {
      socket = new Socket(dest.getAddress(), dest.getPort());
    } catch (IOException e) {
      throw new IOException("Socket to: " + dest + " could not be created.", e);
    }
    return socket;
  }

}
