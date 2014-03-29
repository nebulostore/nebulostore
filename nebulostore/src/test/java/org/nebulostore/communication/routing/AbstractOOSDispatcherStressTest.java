package org.nebulostore.communication.routing;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.messages.CommMessage;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test of {@link OOSDispatcher} which tests multiple concurrent uses.
 *
 * @author Grzegorz Milka
 *
 */
public abstract class AbstractOOSDispatcherStressTest {
  private static final int FIRST_LISTEN_PORT = 3002;
  private static final int NUM_LISTENERS = 4;
  private static final int NUM_SENDERS = 100;

  protected OOSDispatcher dispatcher_;
  private ExecutorService senderExecutor_;
  private ExecutorService listenerExecutor_;

  @Before
  public void setUp() {
    dispatcher_.startUp();
    senderExecutor_ = Executors.newFixedThreadPool(NUM_SENDERS);
    listenerExecutor_ = Executors.newFixedThreadPool(NUM_LISTENERS);
  }

  @After
  public void tearDown() {
    dispatcher_.shutDown();
  }

  /**
   * Runs NUM_LISTENERS independent listeners and send to them NUM_SENDERS messages.
   * All done using {@Link OOSDispatcher}.
   *
   * @author Grzegorz Milka
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout = 1000)
  public void testGet() throws Exception {
    Collection<Listener> listeners = new LinkedList<>();
    Collection<InetSocketAddress> recipients = new LinkedList<>();

    initializeListeners(listeners, recipients, NUM_LISTENERS);
    initializeSenders(recipients, NUM_SENDERS);

    joinTestThreads();

    /* Check results */
    for (Listener listener: listeners) {
      assertEquals(NUM_SENDERS, listener.getCount());
    }
  }

  private void initializeListeners(Collection<Listener> listeners,
      Collection<InetSocketAddress> recipients,
      int howMany) throws Exception {
    for (int i = 0; i < howMany; ++i) {
      Listener listener = new Listener(FIRST_LISTEN_PORT + i);
      listenerExecutor_.execute(listener);
      listeners.add(listener);
      recipients.add(new InetSocketAddress(FIRST_LISTEN_PORT + i));
    }
  }

  private void initializeSenders(Collection<InetSocketAddress> recipients, int howMany) {
    Collection<Runnable> senders = new LinkedList<>();
    for (int i = 0; i < howMany; ++i) {
      List<InetSocketAddress> copyRecipients = new ArrayList<>(recipients);
      Collections.shuffle(copyRecipients);
      Sender sender = new Sender(dispatcher_, copyRecipients);
      senders.add(sender);
    }
    for (Runnable runnable: senders) {
      senderExecutor_.execute(runnable);
    }
  }

  private void joinTestThreads() throws InterruptedException {
    senderExecutor_.shutdown();
    senderExecutor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    listenerExecutor_.shutdown();
    listenerExecutor_.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }
  /**
   * Listens for messages using {@Link ListenerService} and checks their count
   * as well as correctness.
   *
   * @author Grzegorz Milka
   */
  private static class Listener implements Runnable {
    private int listenPort_;
    private int count_;

    public Listener(int listenPort) {
      listenPort_ = listenPort;
    }

    public int getCount() {
      return count_;
    }

    public void run() {
      BlockingQueue<CommMessage> queue = new LinkedBlockingQueue<>();
      Executor executor = Executors.newSingleThreadExecutor();
      ExecutorService workExecutor = Executors.newCachedThreadPool();
      ListenerService listener = new ListenerService(listenPort_, queue, executor, workExecutor);
      try {
        listener.start();
      } catch (IOException e1) {
        return;
      }

      while (count_ != NUM_SENDERS) {
        try {
          queue.take();
          count_ += 1;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }

      listener.stop();
    }
  }

  /**
   * Sends {@link HolderAdvertisementMessage} messages to given recipients.
   *
   * @author Grzegorz Milka
   */
  private static class Sender implements Runnable {
    private static final CommMessage MSG = mock(CommMessage.class);
    private final OOSDispatcher dispatcher_;
    private final Collection<InetSocketAddress> recipients_;

    public Sender(OOSDispatcher dispatcher, Collection<InetSocketAddress> recipients) {
      dispatcher_ = dispatcher;
      recipients_ = recipients;
    }

    public void run() {
      for (InetSocketAddress recipient: recipients_) {
        ObjectOutputStream oos = null;
        try {
          oos = dispatcher_.getStream(recipient);
          oos.writeObject(MSG);
          oos.flush();
        } catch (IOException | InterruptedException e) {
          return;
        } finally {
          if (oos != null) {
            dispatcher_.putStream(recipient);
          }
        }
      }
    }
  }

}
