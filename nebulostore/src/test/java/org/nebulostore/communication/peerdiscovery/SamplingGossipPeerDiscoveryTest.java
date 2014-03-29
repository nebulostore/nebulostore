package org.nebulostore.communication.peerdiscovery;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.peerdiscovery.PeerGossipMessage.MessageType;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.communication.routing.MessageMatcher;
import org.nebulostore.communication.routing.Router;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Grzegorz Milka
 */
public class SamplingGossipPeerDiscoveryTest {
  private static final CommAddress LOCAL_COMM_ADDRESS = new CommAddress(0, 0);
  private static final CommAddress BOOTSTRAP_COMM_ADDRESS = new CommAddress(0, 1);
  private SamplingGossipPeerDiscovery peerDiscovery_;
  private Router router_;

  @Before
  public void setUp() {
    Collection<CommAddress> bootstrapCommAddresses = new LinkedList<>();
    bootstrapCommAddresses.add(BOOTSTRAP_COMM_ADDRESS);
    router_ = mock(Router.class);

    ExecutorService mainExecutor = Executors.newFixedThreadPool(1);
    ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    peerDiscovery_ = new SamplingGossipPeerDiscovery(LOCAL_COMM_ADDRESS,
        bootstrapCommAddresses, router_, mainExecutor, scheduledExecutor);
  }

  @Test
  public void shouldNotifyWhenNewPeerIsFound() throws InterruptedException {
    CommAddress thirdCommAddress = new CommAddress(0, 2);
    PeerGossipMessage gossipMessage = createPushPeerGossipMessage(thirdCommAddress);
    doAnswer(new SingleGossipMessageAnswer(gossipMessage)).when(router_).addMessageListener(
        any(MessageMatcher.class), any(MessageListener.class));

    BlockingQueue<CommAddress> reportQueue = new LinkedBlockingQueue<>();
    Observer obs = new QueueReportingObserver(reportQueue);
    peerDiscovery_.addObserver(obs);
    peerDiscovery_.startUp();

    while (!reportQueue.take().equals(thirdCommAddress)) {
      continue;
    }

    peerDiscovery_.shutDown();
    peerDiscovery_.deleteObserver(obs);

  }

  private PeerGossipMessage createPushPeerGossipMessage(CommAddress newPeer) {
    Set<MessageType> msgType = new HashSet<>();
    msgType.add(MessageType.PUSH);
    Collection<PeerDescriptor> buffer = new LinkedList<>();
    buffer.add(new PeerDescriptor(newPeer));
    return new PeerGossipMessage(newPeer, LOCAL_COMM_ADDRESS, msgType, buffer);
  }

  /**
   * Answer which sends PeerGossipMessage on listener register.
   *
   * @author Grzegorz Milka
   */
  private static class SingleGossipMessageAnswer implements Answer<Void> {
    private final PeerGossipMessage gossipMessage_;

    public SingleGossipMessageAnswer(PeerGossipMessage gossipMessage) {
      gossipMessage_ = gossipMessage;
    }

    @Override
    public Void answer(InvocationOnMock invocation) throws Throwable {
      MessageListener listener = (MessageListener) invocation.getArguments()[1];
      listener.onMessageReceive(gossipMessage_);
      return null;
    }
  }
  /**
   * @author Grzegorz Milka
   */
  private static class QueueReportingObserver implements Observer {
    private final BlockingQueue<CommAddress> reportQueue_;
    public QueueReportingObserver(BlockingQueue<CommAddress> reportQueue) {
      reportQueue_ = reportQueue;
    }

    @Override
    public void update(Observable arg0, Object arg1) {
      @SuppressWarnings("unchecked")
      Collection<CommAddress> newPeers = (Collection<CommAddress>) arg1;
      for (CommAddress newPeer : newPeers) {
        reportQueue_.add(newPeer);
      }
    }
  }
}
