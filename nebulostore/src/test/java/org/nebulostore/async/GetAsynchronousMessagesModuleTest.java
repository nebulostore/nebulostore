package org.nebulostore.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Ignore;
import org.junit.Test;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.messages.GotAsynchronousMessagesMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobEndedMessage;
import org.nebulostore.dispatcher.JobInitMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Simple unit test for GetAsynchronousMessagesModule.
 * @author szymonmatejczyk
 */
public final class GetAsynchronousMessagesModuleTest {

  @Test
  @Ignore
  public void testSimple() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> inQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> outQueue = new LinkedBlockingQueue<Message>();

    // TODO(szymonmatejczyk): This seems to be broken now.
    CommAddress synchroPeerAddress = CommAddress.getZero();
    GetAsynchronousMessagesModule module = new GetAsynchronousMessagesModule(networkQueue,
        outQueue, synchroPeerAddress);
    module.setInQueue(inQueue);
    module.setOutQueue(outQueue);

    new Thread(module).start();

    Message msg;

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      assert false;
      return;
    }
    assertTrue(msg instanceof JobInitMessage);

    String jobId = ((JobInitMessage) msg).getId();

    inQueue.add(msg);

    try {
      msg = networkQueue.take();
    } catch (InterruptedException exception) {
      assert false;
      return;
    }

    assertTrue(msg instanceof GetAsynchronousMessagesMessage);
    GetAsynchronousMessagesMessage gam = (GetAsynchronousMessagesMessage) msg;
    assertEquals(gam.getId(), jobId);
    assertTrue(gam.getDestinationAddress() == synchroPeerAddress);
    //assertTrue(gam.getRecipient() == BrokerContext.getInstance().instanceID_);

    List<AsynchronousMessage> list = new LinkedList<AsynchronousMessage>();
    AsynchronousMessagesMessage messages = new AsynchronousMessagesMessage(jobId, null, null,
        list);
    inQueue.add(messages);

    try {
      msg = networkQueue.take();
    } catch (InterruptedException exception) {
      assert false;
      return;
    }

    assertTrue(msg instanceof GotAsynchronousMessagesMessage);
    assertEquals(((GotAsynchronousMessagesMessage) msg).getId(), jobId);

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      assert false;
      return;
    }

    assertTrue(msg instanceof AsynchronousMessagesMessage);
    AsynchronousMessagesMessage m = (AsynchronousMessagesMessage) msg;
    assert m.getId().equals("parentId");
    assert m.getMessages() == list;

    try {
      msg = outQueue.take();
    } catch (InterruptedException exception) {
      assert false;
      return;
    }

    assertTrue(msg instanceof JobEndedMessage);
    assertEquals(((JobEndedMessage) msg).getId(), jobId);
  }

}
