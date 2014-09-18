package org.nebulostore.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.messages.GotAsynchronousMessagesMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobEndedMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.Timer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple unit test for GetAsynchronousMessagesModule.
 *
 * @author szymonmatejczyk
 */
public final class GetAsynchronousMessagesModuleTest {

  @Mock
  private Timer timer_;

  @Before
  public void beforeTests() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testSimple() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> inQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> outQueue = new LinkedBlockingQueue<Message>();

    CommAddress synchroPeerAddress = CommAddress.getZero();
    GetAsynchronousMessagesModule module = new GetAsynchronousMessagesModule(networkQueue,
        outQueue, synchroPeerAddress, CommAddress.getZero());
    module.setInQueue(inQueue);
    module.setOutQueue(outQueue);
    module.setDependencies(CommAddress.getZero(), timer_);

    new Thread(module).start();
    module.runThroughDispatcher();

    Message msg;

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      fail();
      return;
    }
    assertTrue(msg instanceof JobInitMessage);

    String jobId = ((JobInitMessage) msg).getId();
    inQueue.add(msg);


    try {
      msg = networkQueue.take();
    } catch (InterruptedException exception) {
      fail();
      return;
    }

    assertTrue(msg instanceof GetAsynchronousMessagesMessage);
    GetAsynchronousMessagesMessage gam = (GetAsynchronousMessagesMessage) msg;
    assertEquals(gam.getId(), jobId);
    assertTrue(gam.getDestinationAddress() == synchroPeerAddress);
    //assertTrue(gam.getRecipient() == BrokerContext.getInstance().instanceID_);

    List<AsynchronousMessage> list = new LinkedList<>();
    AsynchronousMessagesMessage messages = new AsynchronousMessagesMessage(jobId, null, null,
        list, CommAddress.getZero());
    inQueue.add(messages);

    try {
      msg = networkQueue.take();
    } catch (InterruptedException exception) {
      fail();
      return;
    }

    assertTrue(msg instanceof GotAsynchronousMessagesMessage);
    assertEquals(((GotAsynchronousMessagesMessage) msg).getId(), jobId);

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      fail();
      return;
    }

    assertTrue(msg instanceof AsynchronousMessagesMessage);
    AsynchronousMessagesMessage m = (AsynchronousMessagesMessage) msg;
    //assert m.getId().equals("parentId");
    assert m.getMessages() == list;

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      fail();
      return;
    }

    assertTrue(msg instanceof JobEndedMessage);
    assertEquals(((JobEndedMessage) msg).getId(), jobId);
  }
}
