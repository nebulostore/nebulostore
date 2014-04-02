package org.nebulostore.communication.peerdiscovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Observable;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.communication.routing.MessageMatcher;
import org.nebulostore.communication.routing.Router;
import org.nebulostore.communication.routing.SendResult;

/**
 * Gossip service that ensures uniform gossiping between peers. It is parameterized by:
 * <b>N</b> - number of peers in the system, excluding server, and <b>K</b> - number of addresses
 * that every peer will receive. It works as follows.
 *
 * <ol>
 * <li>Bootstrap peer acts as a server. It waits for other peers to contact him with gossip
 *     messages.</li>
 * <li>Other peers (clients) send gossip messages to server with single address inside.</li>
 * <li>After server has received information from all N peers, it sends K random contact addresses
 * to every peer, ensuring uniform load.</li>
 * <li>Server also sends CommPeerFound messages to itself so broker should account for them.</li>
 * </ol>
 */
public class OneTimeUniformGossipPeerDiscovery extends Observable
                                               implements PeerDiscovery, Runnable {
  private static final Logger LOGGER = Logger.getLogger(OneTimeUniformGossipPeerDiscovery.class);
  private final Random random_;

  private static final int MAX_PUSH_ATTEMPTS = 3;
  private static final int RESEND_DELAY = 1_500;

  private final CommAddress localCommAddress_;
  private final List<CommAddress> bootstrapCommAddresses_;

  private final int nPeers_;
  private final int groupSize_;
  private boolean isServer_;

  private final Router router_;
  private final BlockingQueue<SendResult> resultQueue_;
  private MessageListener gossipMsgListener_;

  private final ExecutorService mainExecutor_;
  private Future<?> oneTimeGossipTask_;

  /**
   * @param nPeers Number of peers server is waiting for.
   * @param groupSize Number of other peers' addresses that every peer will receive.
   */
  @AssistedInject
  public OneTimeUniformGossipPeerDiscovery(
      @Named("communication.local-comm-address") CommAddress localCommAddress,
      @Assisted("communication.bootstrap-comm-addresses")
        Collection<CommAddress> bootstrapCommAddresses,
      @Named("systest.num-test-participants") int nPeers,
      @Named("communication.one-time-uniform-gossip-n-replicators") int groupSize,
      Router router,
      @Named("communication.peerdiscovery.service-executor") ExecutorService mainExecutor,
      @Named("communication.boostrap.is-server") Boolean isServer, Random random) {
    localCommAddress_ = localCommAddress;
    bootstrapCommAddresses_ = new ArrayList<>(bootstrapCommAddresses);
    nPeers_ = nPeers;
    groupSize_ = groupSize;
    router_ = router;
    mainExecutor_ = mainExecutor;
    isServer_ = isServer;
    random_ = random;

    resultQueue_ = new LinkedBlockingQueue<SendResult>();
  }

  @Override
  public void startUp() {
    LOGGER.debug("startUp()");
    addGossipMessageListener();
    oneTimeGossipTask_ = mainExecutor_.submit(this);
  }

  @Override
  public void shutDown() {
    LOGGER.debug("shutDown()");
    try {
      oneTimeGossipTask_.get();
    } catch (InterruptedException | ExecutionException exception) {
      LOGGER.warn(exception.getMessage());
    }
    deleteGossipMessageListener();
  }

  @Override
  public void run() {
    if (!isServer_) {
      triggerOneTimeGossip();
    }
  }

  private void triggerOneTimeGossip() {
    int nPushAttempts = 0;
    boolean sendResult = false;

    while (!sendResult && nPushAttempts < MAX_PUSH_ATTEMPTS) {
      sendResult = sendLocalAddressToBootstrap();
      ++nPushAttempts;

      if (sendResult) {
        LOGGER.debug("Successfully sent initial gossip message. " +
                     "MessageListener should expect PeerGossipMessage now.");
      } else {
        if (nPushAttempts < MAX_PUSH_ATTEMPTS) {
          LOGGER.debug("Sending initial gossip message didn't succeed. Trying again...");
          try {
            Thread.sleep(RESEND_DELAY);
          } catch (InterruptedException exception) {
            LOGGER.warn(
                "Interrupted while sleeping before another attempt to send initial gossip msg." +
                exception.getMessage());
          }
        } else {
          LOGGER.warn(String.format(
              "Couldn't connect to bootstrap. Stopped trying after %d attempts.", nPushAttempts));
        }
      }
    }
  }

  private boolean sendLocalAddressToBootstrap() {
    LOGGER.trace("Sending local CommAddress to Bootstrap.");

    if (!bootstrapCommAddresses_.isEmpty()) {
      CommAddress bootstrapCommAddress = bootstrapCommAddresses_.get(0);
      PeerDescriptor localPeer = new PeerDescriptor(localCommAddress_);

      PeerGossipMessage checkInMessage = new PeerGossipMessage(localCommAddress_,
          bootstrapCommAddress, EnumSet.of(PeerGossipMessage.MessageType.PUSH),
          Collections.singletonList(localPeer));

      router_.sendMessage(checkInMessage, resultQueue_);
    } else {
      LOGGER.warn("No boostrap address to communicate to.");
    }

    return getSendResult();
  }

  private boolean getSendResult() {
    SendResult result;
    try {
      result = resultQueue_.take();
      return result.getResult();
    } catch (InterruptedException exception) {
      LOGGER.warn("Interrupted while waiting for send result: " + exception.getMessage());
      return false;
    } catch (AddressNotPresentException e) {
      LOGGER.warn("AddressNotPresentException while waiting for send result: " + e.getMessage());
      return false;
    } catch (IOException e) {
      LOGGER.warn("IOException while waiting for send result: " + e.getMessage());
      return false;
    }
  }

  private void addGossipMessageListener() {
    if (isServer_) {
      LOGGER.debug("addGossipMessageListener() -> " +
                                            "initializing OneTimeUniformPeerDiscovery as a server");
      gossipMsgListener_ = new ServerGossipMessageListener();
    } else {
      LOGGER.debug("addGossipMessageListener() -> " +
                                            "initializing OneTimeUniformPeerDiscovery as a client");
      gossipMsgListener_ = new ClientGossipMessageListener();
    }

    router_.addMessageListener(new GossipMessageMatcher(), gossipMsgListener_);
  }

  private void deleteGossipMessageListener() {
    router_.removeMessageListener(gossipMsgListener_);
  }

  private class ServerGossipMessageListener implements MessageListener {
    private List<CommAddress> knownPeers_ = new ArrayList<CommAddress>();

    @Override
    public void onMessageReceive(Message message) {
      LOGGER.debug(String.format("onMessageReceive(%s)", message.toString()));
      PeerGossipMessage peerGossipMessage = (PeerGossipMessage) message;
      CommAddress newPeerAddress = peerGossipMessage.getBuffer().get(0).getPeerAddress();
      notifyServersObservers(newPeerAddress);

      synchronized (knownPeers_) {
        knownPeers_.add(newPeerAddress);
        if (knownPeers_.size() == nPeers_) {
          distributeAddressesUniformly();
        }
      }
    }

    private void notifyServersObservers(CommAddress newPeerAddress) {
      LOGGER.trace(String.format("notifyServersOwnObservers() -> notifying about new peer."));
      setChanged();
      notifyObservers(Collections.singletonList(newPeerAddress));
    }

    private void distributeAddressesUniformly() {
      int actualGroupSize = Math.min(groupSize_, nPeers_ - 1);
      Collections.shuffle(knownPeers_, random_);

      for (int recipientIndex = 0; recipientIndex < nPeers_; ++recipientIndex) {
        List<PeerDescriptor> group = new LinkedList<PeerDescriptor>();
        for (int indexOffset = 1; indexOffset <= actualGroupSize; ++indexOffset) {
          int groupMemberIndex = (recipientIndex + indexOffset) % nPeers_;
          group.add(new PeerDescriptor(knownPeers_.get(groupMemberIndex)));
        }

        PeerGossipMessage peerGroupMessage =
            new PeerGossipMessage(null, knownPeers_.get(recipientIndex),
                                  Collections.singleton(PeerGossipMessage.MessageType.PUSH), group);

        LOGGER.trace("distributeAddressesUniformly() -> router_.sendMessage");
        router_.sendMessage(peerGroupMessage);
      }
    }
  }

  private class ClientGossipMessageListener implements MessageListener {
    @Override
    public void onMessageReceive(Message message) {
      LOGGER.debug(String.format("onMessageReceive(%s)", message.toString()));
      PeerGossipMessage peerGossipMessage = (PeerGossipMessage) message;
      Collection<CommAddress> knownPeers =
          peerDescriptorsToCommAddresses(peerGossipMessage.getBuffer());
      setChanged();
      LOGGER.
        trace(String.format("onMessageReceive() -> notifying about %d peers.", knownPeers.size()));
      notifyObservers(knownPeers);
    }
  }

  private Collection<CommAddress> peerDescriptorsToCommAddresses(Collection<PeerDescriptor> peers) {
    Collection<CommAddress> addresses = new ArrayList<>();
    for (PeerDescriptor peer : peers) {
      addresses.add(peer.getPeerAddress());
    }
    return addresses;
  }

  private static class GossipMessageMatcher implements MessageMatcher {
    @Override
    public boolean matchMessage(CommMessage msg) {
      LOGGER.trace("Checking: " + msg + ", " + (msg instanceof PeerGossipMessage));
      return msg instanceof PeerGossipMessage;
    }
  }
}
