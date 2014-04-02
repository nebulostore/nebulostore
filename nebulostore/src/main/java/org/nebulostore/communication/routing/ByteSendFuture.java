package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Future representing result of byte array send operation.
 *
 * @see Future for semantics
 *
 * @author Grzegorz Milka
 */
public interface ByteSendFuture {
  boolean cancel(boolean mayInterruptIfRunning);

  SendOperationIdentifier getId();

  /**
   *
   * @return true iff operation was successful
   * @throws CancellationException
   * @throws InterruptedException
   * @throws IOException
   */
  boolean get() throws InterruptedException, IOException;

  /**
   *
   * @param timeout
   * @param unit
   * @return true iff operation was successful
   * @throws CancellationException
   * @throws InterruptedException
   * @throws IOException
   * @throws TimeoutException
   */
  boolean get(long timeout, TimeUnit unit) throws InterruptedException, IOException,
      TimeoutException;

  boolean isCancelled();

  boolean isDone();
}
