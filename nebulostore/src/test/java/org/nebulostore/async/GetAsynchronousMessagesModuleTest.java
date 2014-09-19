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
  public void shouldReceiveMessagesAndEndSuccessfully() throws InterruptedException {
    GetAsyncMessagesModuleWrapper wrapper = createWrapper();
    JobInitMessage initMessage = wrapper.startModule();
    String jobId = initMessage.getId();
    getRequest(wrapper, jobId);
    List<AsynchronousMessage> list = sendEmptyListOfMessages(wrapper, jobId);

    Message msg = wrapper.networkQueue_.take();
    assertTrue(msg instanceof GotAsynchronousMessagesMessage);
    assertEquals(((GotAsynchronousMessagesMessage) msg).getId(), jobId);

    msg = wrapper.outQueue_.take();
    assertTrue(msg instanceof AsynchronousMessagesMessage);
    AsynchronousMessagesMessage m = (AsynchronousMessagesMessage) msg;
    assertTrue(m.getMessages() == list);

    msg = wrapper.outQueue_.take();
    assertTrue(msg instanceof JobEndedMessage);
    assertEquals(((JobEndedMessage) msg).getId(), jobId);
  }

  private GetAsyncMessagesModuleWrapper createWrapper() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> inQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> outQueue = new LinkedBlockingQueue<Message>();

    CommAddress synchroPeerAddress = CommAddress.getZero();
    GetAsynchronousMessagesModule module =
        new GetAsynchronousMessagesModule(networkQueue, outQueue, synchroPeerAddress,
            CommAddress.getZero());
    module.setInQueue(inQueue);
    module.setOutQueue(outQueue);
    module.setDependencies(CommAddress.getZero(), timer_);
    return new GetAsyncMessagesModuleWrapper(module, networkQueue, inQueue, outQueue,
        synchroPeerAddress);
  }

  private void getRequest(GetAsyncMessagesModuleWrapper wrapper, String jobId)
      throws InterruptedException {
    Message msg = wrapper.networkQueue_.take();
    assertTrue(msg instanceof GetAsynchronousMessagesMessage);
    GetAsynchronousMessagesMessage gam = (GetAsynchronousMessagesMessage) msg;
    assertEquals(gam.getId(), jobId);
    assertTrue(gam.getDestinationAddress() == wrapper.synchroPeerAddress_);
  }

  private List<AsynchronousMessage> sendEmptyListOfMessages(GetAsyncMessagesModuleWrapper wrapper,
      String jobId) {
    List<AsynchronousMessage> list = new LinkedList<>();
    AsynchronousMessagesMessage messages =
        new AsynchronousMessagesMessage(jobId, null, null, list, CommAddress.getZero());
    wrapper.inQueue_.add(messages);
    return list;
  }

  private class GetAsyncMessagesModuleWrapper {
    public final GetAsynchronousMessagesModule module_;
    public final BlockingQueue<Message> networkQueue_;
    public final BlockingQueue<Message> inQueue_;
    public final BlockingQueue<Message> outQueue_;
    public final CommAddress synchroPeerAddress_;

    public GetAsyncMessagesModuleWrapper(GetAsynchronousMessagesModule module,
        BlockingQueue<Message> networkQueue, BlockingQueue<Message> inQueue,
        BlockingQueue<Message> outQueue, CommAddress synchroPeerAddress) {
      module_ = module;
      networkQueue_ = networkQueue;
      inQueue_ = inQueue;
      outQueue_ = outQueue;
      synchroPeerAddress_ = synchroPeerAddress;
    }

    public JobInitMessage startModule() throws InterruptedException {
      new Thread(module_).start();
      module_.runThroughDispatcher();
      Message msg = outQueue_.take();
      inQueue_.add(msg);
      return (JobInitMessage) msg;
    }

  }
}
