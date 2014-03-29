package org.nebulostore.networkmonitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.messages.RandomPeersSampleMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Gossiping between peers to exchange random peers sample.
 *
 * @author szymonmatejczyk
 *
 */
public class RandomPeersGossipingModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RandomPeersGossipingModule.class);

  private static final long TIMEOUT_MILLIS = 3000L;
  public static final int RANDOM_PEERS_SAMPLE_SIZE = 4;

  private final RPGVisitor visitor_ = new RPGVisitor();
  private CommAddress myAddress_;
  private Timer timer_;

  private NetworkMonitor networkMonitor_;

  CommAddress remotePeer_;

  @Inject
  void setCommAddress(CommAddress myAddress, NetworkMonitor networkMonitor) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
  }

  @Inject
  public void setTimer(Timer timer) {
    timer_ = timer;
  }

  /**
   * Visitor. Implements gossiping a number of random peers protocol.
   */
  public class RPGVisitor extends MessageVisitor<Void> {
    private boolean activeMode_;

    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      // starting in active mode
      activeMode_ = true;
      Set<CommAddress> view = getView();
      if (view.isEmpty()) {
        logger_.debug("Empty view");
        endJobModule();
        return null;
      }

      logger_.debug("Gossiping (actively) started...");

      // sample peer other than this instance
      Integer randomPeerNo = (new Random()).nextInt(view.size());
      int i = 0;
      Iterator<CommAddress> it = view.iterator();
      while (i < randomPeerNo) {
        it.next();
        i++;
      }
      remotePeer_ = it.next();

      view.add(myAddress_);
      networkQueue_.add(new RandomPeersSampleMessage(jobId_, remotePeer_, view));
      timer_.schedule(jobId_, TIMEOUT_MILLIS);
      return null;
    }

    public Void visit(RandomPeersSampleMessage message) {
      Set<CommAddress> view = getView();
      if (activeMode_) {
        view.addAll(message.getPeersSet());
        view = selectView(view);
        networkMonitor_.setRandomPeersSample(view);
        logger_.debug("Gossiping finished.");
      } else {
        jobId_ = message.getId();
        logger_.debug("Received gossiping message.");
        view.add(myAddress_);
        networkQueue_.add(new RandomPeersSampleMessage(message.getId(), message.getSourceAddress(),
            view));
        view.addAll(message.getPeersSet());
        view = selectView(view);
      }
      updateStatistics(view);
      timer_.cancelTimer();
      endJobModule();
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      logger_.warn("Received ErrorCommMessage ", message.getNetworkException());
      timer_.cancelTimer();
      endJobModule();
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout - Random peer: " + remotePeer_ + " not responding...");
      timer_.cancelTimer();
      endJobModule();
      return null;
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Updates statistics in DHT by perfoming connection tests for peers in peers.
   */
  public void updateStatistics(Set<CommAddress> peers) {
    for (CommAddress peer : peers) {
      if (!peer.equals(myAddress_)) {
        new TestPeersConnectionModule(peer, outQueue_);
      }
    }
  }

  /**
   * Filters elements of view so that result is a representative group of peers not larger than
   * RANDOM_PEERS_SAMPLE_SIZE.
   */
  public Set<CommAddress> selectView(Set<CommAddress> view) {
    view.remove(myAddress_);
    // For now only taking random peers from view.
    List<CommAddress> v = new ArrayList<CommAddress>(view);
    Collections.shuffle(v);
    return new TreeSet<CommAddress>(v.subList(0, Math.min(RANDOM_PEERS_SAMPLE_SIZE, v.size())));
  }

  /**
   * Returns a clone of random peers TreeSet from NetworkContext. If no peers are stored in
   * NetworkContext waits for them.
   */
  protected Set<CommAddress> getView() {
    Set<CommAddress> set = new TreeSet<CommAddress>(networkMonitor_.getRandomPeersSample());
    return set;
  }
}
