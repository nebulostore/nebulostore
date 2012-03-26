package org.nebulostore.dispatcher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.nebulostore.appcore.JobModule;
import org.nebulostore.appcore.Message;
import org.nebulostore.dispatcher.Dispatcher;
import org.nebulostore.dispatcher.messages.JobEndedMessage;
import org.nebulostore.dispatcher.messages.KillDispatcherMessage;

import static org.junit.Assert.assertTrue;

/**
 * Dispatcher test class.
 */
public class DispatcherTest {
  private BlockingQueue<Message> inQueue_;
  private BlockingQueue<Message> outQueue_;
  private Dispatcher dispatcher_;
  private Thread thread_;

  public static int staticCounter_;

  @Before
  public void setUp() throws Exception {
    inQueue_ = new LinkedBlockingQueue<Message>();
    outQueue_ = new LinkedBlockingQueue<Message>();

    dispatcher_ = new Dispatcher(inQueue_, outQueue_);
    thread_ = new Thread(dispatcher_);
    thread_.start();
  }

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Adds two messages with the same ID to dispatcher and checks if the same
   * thread handles both of them (via static variable). Then send a
   * JobEndMessage and verify that it was not forwarded to the worker thread.
   */
  @Test
  public void testHandlerExists() {
    /**
     * Simple module that counts messages.
     */
    class DummyModule extends JobModule {
      private int nMsgs_;

      DummyModule() {
        nMsgs_ = 0;
      }

      public void processMessage(Message message) {
        staticCounter_++;
        nMsgs_++;
        // Check if we are the only message receiver.
        assertTrue(staticCounter_ == nMsgs_);
      }

      @Override
      public void run() {
        try {
          processMessage(inQueue_.take());
          processMessage(inQueue_.take());
        } catch (InterruptedException exception) {
          return;
        }
      }
    }
    /**
     * Dummy message.
     */
    class DummyMessage extends Message {
      public DummyMessage() {
        super("1");
      }

      @Override
      public JobModule getHandler() {
        return new DummyModule();
      }
    }

    staticCounter_ = 0;
    inQueue_.add(new DummyMessage());
    inQueue_.add(new DummyMessage());
    inQueue_.add(new JobEndedMessage("1"));
    inQueue_.add(new KillDispatcherMessage());
    try {
      thread_.join();
    } catch (InterruptedException exception) {
      assertTrue(false);
    }
    // Verify that only two messages were handled by a worker thread.
    assertTrue(staticCounter_ == 2);
  }
}
