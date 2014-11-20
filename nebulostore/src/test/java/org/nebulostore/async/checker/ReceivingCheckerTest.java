package org.nebulostore.async.checker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.errorresponder.ErrorMessageErrorResponder;
import org.nebulostore.communication.routing.errorresponder.ErrorResponder;
import org.nebulostore.communication.routing.errorresponder.MessageNotReceivedMessage;
import org.nebulostore.timer.TimerImpl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReceivingCheckerTest {
  private static final long ACK_TIMEOUT_MILIS = MessageReceivingCheckerModule.ACK_TIMEOUT_MILIS +
      2 * MessageReceivingCheckerModule.TICK_PERIOD_MILIS;

  private static CommAddress sourceAddress_;
  private static CommAddress destAddress_;

  @BeforeClass
  public static void before() throws NoSuchFieldException {
    sourceAddress_ = CommAddress.newRandomCommAddress();
    destAddress_ = CommAddress.newRandomCommAddress();
  }

  public CheckerModuleWrapper createWrapper(CommAddress sourceAddress) {
    BlockingQueue<Message> inQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Message> outQueue = new LinkedBlockingQueue<>();
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<>();
    CheckerModuleWrapper wrapper = new CheckerModuleWrapper(inQueue, outQueue, networkQueue);
    wrapper.initModule(sourceAddress);
    return wrapper;
  }

  @Test
  public void shouldRespondWithAckMessage() throws InterruptedException {
    CheckerModuleWrapper wrapper = createWrapper(sourceAddress_);
    Message testMessage = new CheckerTestMessage(destAddress_, sourceAddress_);
    wrapper.inQueue_.add(testMessage);

    Message message = wrapper.networkQueue_.take();
    assertTrue(message instanceof MessageReceivedMessage);
    MessageReceivedMessage msg = (MessageReceivedMessage) message;
    assertTrue(msg.getOriginalMessageId().equals(testMessage.getMessageId()));
    assertTrue(msg.getSourceAddress().equals(sourceAddress_));
    assertTrue(msg.getDestinationAddress().equals(destAddress_));

    message = wrapper.outQueue_.take();
    assertTrue(message.equals(testMessage));
  }

  @Test
  public void shouldNotGenerateErrorResponder() throws InterruptedException {
    CheckerModuleWrapper wrapper = createWrapper(sourceAddress_);
    CommMessage testMessage = new CheckerTestMessage(sourceAddress_, destAddress_);
    wrapper.inQueue_.add(testMessage);

    Message message = wrapper.networkQueue_.take();
    assertTrue(message.equals(testMessage));

    wrapper.inQueue_.add(new MessageReceivedMessage(destAddress_, sourceAddress_, message
        .getMessageId()));

    Message msg = wrapper.outQueue_.poll(ACK_TIMEOUT_MILIS, TimeUnit.MILLISECONDS);
    assertNull(msg);
  }

  @Test
  public void shouldGenerateErrorResponder() throws InterruptedException {
    CheckerModuleWrapper wrapper = createWrapper(sourceAddress_);
    CommMessage testMessage = new CheckerTestMessage(sourceAddress_, destAddress_);
    wrapper.inQueue_.add(testMessage);

    Message message = wrapper.networkQueue_.take();
    assertTrue(message.equals(testMessage));

    Message msg = wrapper.outQueue_.poll(ACK_TIMEOUT_MILIS, TimeUnit.MILLISECONDS);
    assertNotNull(msg);
    assertTrue(msg instanceof MessageNotReceivedMessage);
  }

  private class CheckerModuleWrapper {
    private final BlockingQueue<Message> inQueue_;
    private final BlockingQueue<Message> outQueue_;
    private final BlockingQueue<Message> networkQueue_;
    private MessageReceivingCheckerModule module_;

    public CheckerModuleWrapper(BlockingQueue<Message> inQueue, BlockingQueue<Message> outQueue,
        BlockingQueue<Message> networkQueue) {
      inQueue_ = inQueue;
      outQueue_ = outQueue;
      networkQueue_ = networkQueue;
    }

    public MessageReceivingCheckerModule initModule(CommAddress sourceAddress) {
      module_ =
          new MessageReceivingCheckerModule(new TimerImpl(inQueue_), sourceAddress, networkQueue_,
              outQueue_, inQueue_, outQueue_);
      new Thread(module_).start();
      return module_;
    }
  }

  private class CheckerTestMessage extends CommMessage {

    private static final long serialVersionUID = -3197716495905888890L;

    public CheckerTestMessage(CommAddress sourceAddress, CommAddress destAddress) {
      super(sourceAddress, destAddress);
    }

    @Override
    public boolean requiresAck() {
      return true;
    }

    @Override
    public ErrorResponder generateErrorResponder(BlockingQueue<Message> dispatcherQueue) {
      return new ErrorMessageErrorResponder(dispatcherQueue, this);
    }
  }

}
