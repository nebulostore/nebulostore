package org.nebulostore.communication.peerdiscovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.communication.routing.MessageMatcher;
import org.nebulostore.communication.routing.Router;
import org.nebulostore.communication.routing.SendResult;
import org.nebulostore.communication.routing.SendResult.ResultType;


/**
 * Standard peer gossiping service.
 *
 * Basic implementation of Gossip-based peer sampling based on "Gossip-based Peer Sampling"
 * by Jelasty, Voulgaris...
 * @author Grzegorz Milka
 */
public class SamplingGossipPeerDiscovery extends Observable implements PeerDiscovery, Runnable {
  private static final Logger LOGGER = Logger.getLogger(SamplingGossipPeerDiscovery.class);
  private static final Random RANDOM = new Random();

  private final CommAddress localCommAddress_;
  private final List<CommAddress> bootstrapCommAddresses_;

  private final Router router_;
  private final BlockingQueue<SendResult> resultQueue_;
  private final GossipMessageListener gossipMsgListener_;

  /**
   * Period at which GossipSender sends its advertisements. In milliseconds.
   */
  private final int gossipPeriod_ = 5000;

  /**
   * Equivalent of c in the paper.
   *
   * It has to be greater than 1 (or even 3) to ensure non-empty gossips.
   */
  private final int maxPeersSize_ = 20;

  /**
   * Equivalent of H in the paper.
   */
  private final int healingFactor_ = 1;

  /**
   * Equivalent of S in the paper.
   */
  private final int swappingFactor_ = 5;

  private List<PeerDescriptor> peers_ = Collections.synchronizedList(
      new LinkedList<PeerDescriptor>());

  private final ExecutorService mainExecutor_;
  private final ScheduledExecutorService scheduledExecutor_;
  private Future<?> gossipTask_;

  /**
   * Task running periodical gossip exchange.
   */
  private ScheduledFuture<?> gossipSender_;

  @AssistedInject
  public SamplingGossipPeerDiscovery(
      @Named("communication.local-comm-address") CommAddress localCommAddress,
      @Assisted("communication.bootstrap-comm-addresses")
        Collection<CommAddress> bootstrapCommAddresses,
      Router router,
      @Named("communication.peerdiscovery.service-executor") ExecutorService mainExecutor,
      @Named("communication.peerdiscovery.scheduled-executor")
      ScheduledExecutorService scheduledExecutor) {
    localCommAddress_ = localCommAddress;
    bootstrapCommAddresses_ = new ArrayList<>(bootstrapCommAddresses);
    router_ = router;
    resultQueue_ = new LinkedBlockingQueue<SendResult>();
    mainExecutor_ = mainExecutor;
    scheduledExecutor_ = scheduledExecutor;
    gossipMsgListener_ = new GossipMessageListener();
  }

  @Override
  public void addObserver(Observer o) {
    super.addObserver(o);
  }

  @Override
  public void deleteObserver(Observer o) {
    super.deleteObserver(o);
  }

  /**
   * Get random active peer.
   *
   * It returns null if no peer is present.
   */
  public CommAddress getPeer() {
    PeerDescriptor peer = selectPeer();
    if (peer != null) {
      return peer.getPeerAddress();
    } else {
      return null;
    }
  }

  @Override
  public void run() {
    while (!Thread.interrupted()) {
      SendResult result;
      try {
        result = resultQueue_.take();
      } catch (InterruptedException e) {
        break;
      }
      if (result.getType() == ResultType.ERROR) {
        deleteKnownPeer(result.getMessage().getDestinationAddress());
      }
    }
  }

  @Override
  public synchronized void startUp() {
    LOGGER.debug("startUp()");
    addGossipMessageListener();
    startGossipSender();
    gossipTask_ = mainExecutor_.submit(this);
  }

  @Override
  public synchronized void shutDown() {
    LOGGER.debug("shutDown()");
    gossipTask_.cancel(true);
    try {
      gossipTask_.get();
    } catch (CancellationException e) {
      /* Expected, ignore */
      gossipTask_ = null;
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("Unexpected exception", e);
    }
    stopGossipSender();
    deleteGossipMessageListener();
    scheduledExecutor_.shutdown();
  }

  private void addGossipMessageListener() {
    router_.addMessageListener(new GossipMessageMatcher(), gossipMsgListener_);
  }

  private void deleteKnownPeer(CommAddress peerAddress) {
    synchronized (peers_) {
      peers_.remove(new PeerDescriptor(peerAddress));
    }
  }

  private void deleteGossipMessageListener() {
    router_.removeMessageListener(gossipMsgListener_);
  }

  private void increaseAge() {
    synchronized (peers_) {
      for (PeerDescriptor peer : peers_) {
        peer.setAge(peer.getAge() + 1);
      }
    }
  }

  private Collection<CommAddress> peerDescriptorsToCommAddresses(
      Collection<PeerDescriptor> peers) {
    Collection<CommAddress> addresses = new ArrayList<>();
    for (PeerDescriptor peer : peers) {
      addresses.add(peer.getPeerAddress());
    }
    return addresses;
  }

  /**
   * Constructor of appropriate gossip message to given recipient.
   *
   * @author Grzegorz Milka
   */
  private PeerGossipMessage prepareMsgToSend(CommAddress recipient,
      EnumSet <PeerGossipMessage.MessageType> msgType) {
    if (!msgType.contains(PeerGossipMessage.MessageType.PUSH)) {
      return new PeerGossipMessage(localCommAddress_, recipient, msgType,
          new LinkedList<PeerDescriptor>());
    }

    List<PeerDescriptor> bufferToSend;
    synchronized (peers_) {
      bufferToSend = new ArrayList<PeerDescriptor>(peers_);
    }
    Collections.shuffle(bufferToSend);

    // Finding H oldest address and moving them to the end of list
    if (healingFactor_ < bufferToSend.size()) {
      List<PeerDescriptor> hOldest =
        new ArrayList<PeerDescriptor>(bufferToSend);
      Collections.sort(hOldest, new AgeComparator());
      hOldest.subList(0, hOldest.size() - healingFactor_ - 1).clear();
      bufferToSend.removeAll(hOldest);
      bufferToSend.addAll(hOldest);
    }

    if (bufferToSend.size() > (maxPeersSize_ / 2 - 1)) {
      bufferToSend.subList(maxPeersSize_ / 2 - 1, bufferToSend.size() - 1).clear();
    }

    bufferToSend.add(0, new PeerDescriptor(localCommAddress_));

    return new PeerGossipMessage(localCommAddress_, recipient, msgType, bufferToSend);
  }

  /**
   * Select method from the paper.
   *
   * WARNING: select changes otherPeers argument.
   */
  private void select(Collection<PeerDescriptor> otherPeers) {
    synchronized (peers_) {
      Set<PeerDescriptor> oldPeers = new HashSet<PeerDescriptor>(peers_);

      //Remove myself if present
      otherPeers.remove(new PeerDescriptor(localCommAddress_));

      // Append new list and remove duplicates leaving younger descriptors.
      // Assuming no duplicates in otherPeers.
      for (PeerDescriptor peer : otherPeers) {
        int index = peers_.indexOf(peer);
        if (index != -1) {
          PeerDescriptor duplicatePeer = peers_.get(index);
          if (duplicatePeer.getAge() > peer.getAge()) {
            peers_.remove(index);
          } else {
            continue;
          }
        }
        peers_.add(peer);
      }

      //Delete old items
      List<PeerDescriptor> peerList = new ArrayList<PeerDescriptor>(peers_);
      if (peers_.size() > maxPeersSize_) {
        Collections.sort(peerList, new AgeComparator());
        peerList.subList(0, peerList.size() -
            Math.min(healingFactor_, peers_.size() - maxPeersSize_) - 1).
          clear();
        peers_.removeAll(peerList);
      }

      //Delete head
      //Note: Head in the paper refers to the end of list as implemented here
      final int headRange =
        Math.max(Math.min(swappingFactor_, peers_.size() - maxPeersSize_), 0);
      for (int i = 0; i < headRange; ++i) {
        peers_.remove(peers_.size() - 1);
      }

      //Remove at random
      if (peers_.size() > maxPeersSize_) {
        peerList.clear();
        peerList.addAll(peers_);
        Collections.shuffle(peerList);
        peerList.subList(maxPeersSize_, peers_.size() - 1).clear();
        peers_.retainAll(peerList);
      }

      //make a copy of peers and get diff of new peers
      Set<PeerDescriptor> newPeers = new HashSet<PeerDescriptor>(peers_);
      newPeers.removeAll(oldPeers);
      if (!newPeers.isEmpty()) {
        setChanged();
        LOGGER.trace(String.format("select() -> notifying about %d new peers.", newPeers.size()));
        notifyObservers(peerDescriptorsToCommAddresses(newPeers));
      }
    }
  }

  /**
   * Select peer using rand strategy.
   *
   * It returns null if no peer is present.
   */
  /* NOTE: tail(oldest peer) strategy also works */
  private PeerDescriptor selectPeer() {
    synchronized (peers_) {
      if (peers_.size() == 0) {
        return null;
      }
      int index = RANDOM.nextInt(peers_.size());
      return peers_.get(index);
    }
  }

  private void startGossipSender() {
    gossipSender_ = scheduledExecutor_.scheduleWithFixedDelay(new GossipSender(), 0, gossipPeriod_,
        TimeUnit.MILLISECONDS);
  }

  private void stopGossipSender() {
    gossipSender_.cancel(false);
    try {
      gossipSender_.get();
    } catch (CancellationException e) {
      gossipSender_ = null;
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.warn("GossipSender threw exception when stopping.", e);
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private static class AgeComparator implements Comparator<PeerDescriptor> {
    public int compare(PeerDescriptor a, PeerDescriptor b) {
      int diff = a.getAge() - b.getAge();
      if (diff == 0) {
        return a.getPeerAddress().toString().compareTo(
            b.getPeerAddress().toString());
      } else {
        return diff;
      }
    }

  }

  /**
   * @author Grzegorz Milka
   */
  private class GossipMessageListener implements MessageListener {
    @Override
    public void onMessageReceive(Message msg) {
      LOGGER.debug(String.format("onMessageReceive(%s)", msg.toString()));
      PeerGossipMessage peerGossipMsg = (PeerGossipMessage) msg;
      if (peerGossipMsg.getMsgType().contains(PeerGossipMessage.MessageType.PULL)) {
        router_.sendMessage(prepareMsgToSend(peerGossipMsg.getSourceAddress(),
              EnumSet.of(PeerGossipMessage.MessageType.PUSH)));
      }
      if (peerGossipMsg.getMsgType().contains(PeerGossipMessage.MessageType.PUSH)) {
        select(peerGossipMsg.getBuffer());
      }

      increaseAge();
    }
  }

  /**
   *
   * @author Grzegorz Milka
   *
   */
  private static class GossipMessageMatcher implements MessageMatcher {
    @Override
    public boolean matchMessage(CommMessage msg) {
      LOGGER.trace("Checking: " + msg + ", " + (msg instanceof PeerGossipMessage));
      return msg instanceof PeerGossipMessage;
    }
  }

  /**
   * Task responsible for sending PUSHPULL Gossip every GOSSIP_PERIOD milliseconds.
   *
   * @author Grzegorz Milka
   */
  private class GossipSender implements Runnable {
    @Override
    public void run() {
      LOGGER.trace("GossipSender.run()");
      PeerDescriptor recipient = selectPeer();
      CommAddress bootstrapCommAddress = bootstrapCommAddresses_.get(RANDOM.nextInt(
          bootstrapCommAddresses_.size()));

      if (recipient == null && !bootstrapCommAddress.equals(localCommAddress_)) {
        synchronized (peers_) {
          peers_.add(new PeerDescriptor(bootstrapCommAddress));
          recipient = selectPeer();
          assert recipient != null;
        }
      }

      if (recipient != null) {
        LOGGER.trace("GossipSender.run() -> router_.sendMessage");
        router_.sendMessage(prepareMsgToSend(recipient.getPeerAddress(),
              EnumSet.of(
                PeerGossipMessage.MessageType.PUSH,
                PeerGossipMessage.MessageType.PULL)), resultQueue_);
        recipient.setAge(0);
      }
    }
  }
}
