package org.nebulostore.networkmonitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.GlobalContext;
import org.nebulostore.appcore.JobModule;
import org.nebulostore.appcore.Message;
import org.nebulostore.appcore.MessageVisitor;
import org.nebulostore.appcore.TimeoutMessage;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.CommunicationPeer;
import org.nebulostore.communication.address.CommAddress;
import org.nebulostore.dispatcher.messages.JobInitMessage;
import org.nebulostore.networkmonitor.messages.RandomPeersSampleMessage;
import org.nebulostore.timer.TimerContext;

/**
 * Gossiping between peers to exchange random peers sample.
 * @author szymonmatejczyk
 *
 */
public class RandomPeersGossipingModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RandomPeersGossipingModule.class);

  private static final long TIMEOUT_MILLIS = 5000L;
  public static final int RANDOM_PEERS_SAMPLE_SIZE = 3;

  public static final long INTERVAL = 1000L;

  private final RPGVisitor visitor_ = new RPGVisitor();

  /**
   * Visitor.
   */
  private class RPGVisitor extends MessageVisitor<Void> {
    private boolean activeMode_;
    @Override
    public Void visit(JobInitMessage message) {
      logger_.debug("Gossiping...");
      jobId_ = message.getId();
      // starting in active mode
      activeMode_ = true;
      TreeSet<CommAddress> view = getView();
      if (view.isEmpty()) {
        logger_.debug("Empty view");
        endJobModule();
        return null;
      }

      logger_.debug("Gossiping started...");

      // sample peer other than this instance
      Integer randomPeerNo = (new Random()).nextInt(view.size());
      int i = 0;
      Iterator<CommAddress> it = view.iterator();
      while (i <= randomPeerNo) {
        it.next();
        i++;
      }
      CommAddress remotePeer = it.next();

      view.add(CommunicationPeer.getPeerAddress());
      networkQueue_.add(new RandomPeersSampleMessage(null, remotePeer, view));
      TimerContext.getInstance().notifyWithTimeoutMessageAfter(getJobId(), TIMEOUT_MILLIS);
      return null;
    }

    @Override
    public Void visit(RandomPeersSampleMessage message) {
      TreeSet<CommAddress> view = getView();
      if (activeMode_) {
        view.addAll(message.getPeersSet());
        view = selectView(view);
        NetworkContext.getInstance().setRandomPeersSample(view);
      } else {
        logger_.debug("Received gossiping message.");
        view.add(CommunicationPeer.getPeerAddress());
        networkQueue_.add(new RandomPeersSampleMessage(null, message.getSourceAddress(), view));
        view.remove(CommunicationPeer.getPeerAddress());
        view.addAll(message.getPeersSet());
        view = selectView(view);
      }
      updateStatistics(view);
      TimerContext.getInstance().cancelNotifications(getJobId());
      endJobModule();
      return null;
    }

    @Override
    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout.");
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
      TestPeersConnectionModule testConnection = new TestPeersConnectionModule(peer);
      testConnection.runThroughDispatcher(GlobalContext.getInstance().getDispatcherQueue());
    }
  }

  /**
   * Filters elements of view so that result is a representative group of peers not larger
   * than RANDOM_PEERS_SAMPLE_SIZE.
   */
  public TreeSet<CommAddress> selectView(TreeSet<CommAddress> view) {
    // For now only taking random peers from view.
    ArrayList<CommAddress> v = new ArrayList<CommAddress>(view);
    Collections.shuffle(v);
    return new TreeSet<CommAddress>(v.subList(0, Math.min(RANDOM_PEERS_SAMPLE_SIZE, v.size() - 1)));
  }

  /**
   * Returns a clone of random peers TreeSet from NetworkContext.
   * If no peers are stored in NetworkContext waits for them.
   */
  protected TreeSet<CommAddress> getView() {
    TreeSet<CommAddress> set =
        new TreeSet<CommAddress>(NetworkContext.getInstance().getRandomPeersSample());
    return set;
  }
}