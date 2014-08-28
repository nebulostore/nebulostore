package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.Observer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.nebulostore.communication.naming.AddressNotPresentException;

/**
 * Future representing result of message send operation.
 *
 * @see Future for semantics
 *
 * @author Grzegorz Milka
 */
public interface MessageSendFuture {
  boolean cancel(boolean mayInterruptIfRunning);

  /**
   *
   * @return true iff operation was successful
   * @throws AddressNotPresentException
   * @throws CancellationException
   * @throws InterruptedException
   * @throws IOException
   */
  boolean get() throws AddressNotPresentException, InterruptedException,
      IOException;

  /**
   *
   * @param timeout
   * @param unit
   * @return true iff operation was successful
   * @throws AddressNotPresentException
   * @throws CancellationException
   * @throws InterruptedException
   * @throws IOException
   * @throws TimeoutException
   */
  boolean get(long timeout, TimeUnit unit) throws AddressNotPresentException,
      InterruptedException, IOException, TimeoutException;

  boolean isCancelled();

  boolean isDone();

  SendResult getResult();

  void addObserver(Observer o);
}
