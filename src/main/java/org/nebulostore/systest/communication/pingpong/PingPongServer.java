package org.nebulostore.systest.communication.pingpong;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.address.CommAddress;

/**
 * Server controlling remote peers for ping pong tests.
 * It responsible for collecting information about remote peers. Running
 * pingpong test and shutting down peers afterwards.
 * @author grzegorzmilka
 */
public final class PingPongServer extends TestingServerImpl {
  private Map<Integer, PingPongPeer> peers_ =
    new HashMap<Integer, PingPongPeer>();
  private AtomicBoolean hasStarted_ = new AtomicBoolean(false);
  // 60 seconds
  private static final int WAIT_PERIOD = 60000;
  // 60 seconds
  private static final int PING_DELAY = 60000;

  public PingPongServer() throws NebuloException {
    try {
      peers_.put(0, new PingPongPeerImpl(0));
    } catch (NebuloException e) {
      logger_.error("NebuloException when creating server's peer: " + e);
      throw e;
    }
  }

  @Override
  public void run() {
    logger_.info("Running server. Entering wait period.");
    try {
      Thread.sleep(WAIT_PERIOD);
    } catch (InterruptedException e) {
      logger_.debug("Ignored InterruptedException: " + e);
    }
    logger_.info("Wait period ended.");

    hasStarted_.set(true);

    int pingId = 0;
    for (Map.Entry<Integer, PingPongPeer> entry : peers_.entrySet()) {
      int peerId = entry.getKey();
      PingPongPeer peer = entry.getValue();

      Collection<Integer> expectedRespondents =
        new HashSet<Integer>(peers_.keySet());
      expectedRespondents.remove(peerId);

      try {
        peer.sendPing(pingId);
        logger_.info(String.format("Sent ping of id %2d from peer: %2d", pingId, peerId));
      } catch (RemoteException e) {
        logger_.error(String.format("Could not send ping from peer: %2d," +
              " due to: %s", peerId, e.toString()));
        continue;
      }

      try {
        Thread.sleep(PING_DELAY);
      } catch (InterruptedException e) {
        logger_.info("Ignored.");
      }

      Collection<Integer> respondents;
      try {
        respondents = peer.getRespondents(pingId);
      } catch (RemoteException e) {
        logger_.error(String.format("Could get respondents from peer: %2d," +
              " due to: %s", peerId, e.toString()));
        continue;
      }
      logger_.info(String.format("Peer %2d received response from: %s", peerId,
            respondents.toString()));
      logger_.info(String.format("Responsiveness level is: %d/%d.", respondents.size(),
            expectedRespondents.size()));
      ++pingId;
    }

    for (Map.Entry<Integer, PingPongPeer> entry : peers_.entrySet()) {
      int peerId = entry.getKey();
      PingPongPeer peer = entry.getValue();
      logger_.info("Shutting down peer: " + peerId);
      try {
        peer.stopCommPeer();
      } catch (RemoteException e) {
        logger_.error("Could not shutdown peer: " + peerId + ", " + e);
      }
      logger_.info("Shutdown of: " + peerId + " complete");
    }
    /*} catch (RuntimeException e) {
      logger_.error("Caught RuntimeException: " + e);
      e.printStackTrace();
      }*/
  }

  @Override
  protected boolean addPeer(AbstractPeer peer) throws RemoteException {
    if (!(peer instanceof PingPongPeer)) {
      logger_.warn("Someone tried to add incorrent type of peer: " + peer);
      throw new IllegalArgumentException("Peer: " + peer + " is not PingPongPeer.");
    }
    if (hasStarted_.get())
      return false;
    // Throws RemoteException
    int peerId = peer.getId();
    CommAddress address = peer.getCommAddress();
    if (peers_.containsKey(peerId)) {
      logger_.warn("Someone tried to add peer with duplicate id: " + peerId);
      throw new IllegalArgumentException("Peer: " + peerId + " already present");
    }

    peers_.put(peer.getId(), (PingPongPeer) peer);
    logger_.info(String.format("Peer: %2d with address: %s added to map.",
          peer.getId(), address));
    return true;
  }
}