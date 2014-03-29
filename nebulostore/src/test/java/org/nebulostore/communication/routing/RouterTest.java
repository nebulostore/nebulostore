package org.nebulostore.communication.routing;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Grzegorz Milka
 */
public class RouterTest {
  @Mock
  private ListenerService listener_;
  private BlockingQueue<CommMessage> listeningQueue_;

  @Mock
  private MessageSender msgSender_;
  private Router router_;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    listeningQueue_ = new LinkedBlockingQueue<CommMessage>();
    when(listener_.getListeningQueue()).thenReturn(listeningQueue_);
    ExecutorService executor = Executors.newFixedThreadPool(1);
    router_ = new Router(listener_, msgSender_, new CommAddress(0, 0), executor);
    router_.start();

    verify(listener_).start();
  }

  @After
  public void tearDown() throws Exception {
    router_.shutDown();
    verify(listener_).stop();
    verify(msgSender_).shutDown();
  }

  @Test
  public void shouldForwardMessage() throws Exception {
    MessageMatcher aMatcher = new TaggedMessageMatcher("A");
    MessageMatcher bMatcher = new TaggedMessageMatcher("B");
    BlockingMessageListener aListener = new BlockingMessageListener();
    BlockingMessageListener bListener = new BlockingMessageListener();
    CommMessage aMessage = new TaggedCommMessage(null, null, "A");
    CommMessage bMessage = new TaggedCommMessage(null, null, "B");

    router_.addMessageListener(aMatcher, aListener);
    router_.addMessageListener(bMatcher, bListener);

    listeningQueue_.add(aMessage);
    listeningQueue_.add(bMessage);

    aListener.assertMessageArrival(aMessage);
    bListener.assertMessageArrival(bMessage);

    router_.removeMessageListener(bListener);

    listeningQueue_.add(aMessage);
    listeningQueue_.add(bMessage);

    aListener.assertMessageArrival(aMessage);
  }

  @Test
  public void shouldSendMessage() {
    CommMessage aMessage = new TaggedCommMessage(null, null, "A");
    router_.sendMessage(aMessage);
    verify(msgSender_).sendMessage(eq(aMessage));
  }

  /**
   *
   * @author Grzegorz Milka
   */
  private static class BlockingMessageListener implements MessageListener {
    private final BlockingQueue<Message> queue_ = new LinkedBlockingQueue<>();

    public void assertMessageArrival(CommMessage expectedMsg) throws InterruptedException {
      Message msg = queue_.take();
      assertEquals(expectedMsg, msg);
    }

    @Override
    public void onMessageReceive(Message msg) {
      queue_.add(msg);
    }
  }

  /**
   * CommMessage with tag.
   *
   * @author Grzegorz Milka
   *
   */
  private static class TaggedCommMessage extends CommMessage {
    private static final long serialVersionUID = 1L;
    private final String tag_;
    public TaggedCommMessage(CommAddress sourceAddress, CommAddress destAddress, String tag) {
      super(sourceAddress, destAddress);
      tag_ = tag;
    }

    public String getTag() {
      return tag_;
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private static class TaggedMessageMatcher implements MessageMatcher {
    private final String tag_;

    public TaggedMessageMatcher(String tag) {
      tag_ = tag;
    }

    public boolean matchMessage(CommMessage msg) {
      if (msg instanceof TaggedCommMessage) {
        TaggedCommMessage taggedMsg = (TaggedCommMessage) msg;
        return taggedMsg.getTag().equals(tag_);
      }
      return false;
    }
  }
}
