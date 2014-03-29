package org.nebulostore.networkmonitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestMessage;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.Timer;

/**
 * Basic implementation of NetworkMonitor.
 * @author szymon
 *
 */
public class NetworkMonitorImpl extends NetworkMonitor {
  private final Logger logger_ = Logger.getLogger(NetworkMonitor.class);
  private Timer timer_;

  /**
   * Providers for NetworkMonitor submodules.
   */
  private Provider<RandomPeersGossipingModule> randomPeersGossipingModuleProvider_;

  private static final String CONFIGURAION_PREFIX = "networkmonitor.";

  private final Set<CommAddress> knownPeers_ = new HashSet<CommAddress>();
  private final List<CommAddress> knownPeersVector_ = new Vector<CommAddress>();

  private Set<CommAddress> randomPeersSample_ = new HashSet<CommAddress>();
  private CommAddress commAddress_;

  protected BlockingQueue<Message> dispatcherQueue_;

  protected Provider<ConnectionTestMessageHandler> connectionTestMessageHandlerProvider_;

  private long statisticsUpdateIntervalMillis_;

  protected MessageVisitor<Void> visitor_;

  public NetworkMonitorImpl() {
    visitor_ = new NetworkMonitorVisitor();
  }

  @Inject
  public void setDependencies(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      CommAddress commAddress, Timer timer,
      Provider<RandomPeersGossipingModule> randomPeersGossipingModuleProvider,
      Provider<ConnectionTestMessageHandler> connectionTestMessageHandlerProvider,
      @Named(CONFIGURAION_PREFIX + "statistics-update-interval-millis") long
        statisticsUpdateIntervalMillis) {
    dispatcherQueue_ = dispatcherQueue;
    commAddress_ = commAddress;
    knownPeers_.add(commAddress_);
    knownPeersVector_.add(commAddress_);
    timer_ = timer;
    randomPeersGossipingModuleProvider_ = randomPeersGossipingModuleProvider;
    connectionTestMessageHandlerProvider_ = connectionTestMessageHandlerProvider;
    statisticsUpdateIntervalMillis_ = statisticsUpdateIntervalMillis;
  }

  /**
   * Messages to be send to dispatcher when context changes.
   */
  private final Set<MessageGenerator> contextChangeMessageGenerators_ = Collections
      .newSetFromMap(new ConcurrentHashMap<MessageGenerator, Boolean>());

  private void contextChanged() {
    logger_.debug("context changed");
    for (MessageGenerator m : contextChangeMessageGenerators_) {
      logger_.debug("sending CC message");
      dispatcherQueue_.add(m.generate());
    }
  }

  @Override
  public void addContextChangeMessageGenerator(MessageGenerator generator) {
    logger_.debug("Adding contextChangeMessageGenerator.");
    contextChangeMessageGenerators_.add(generator);
  }

  @Override
  public void removeContextChangeMessageGenerator(MessageGenerator generator) {
    contextChangeMessageGenerators_.remove(generator);
  }

  @Override
  public List<CommAddress> getKnownPeers() {
    return new ArrayList<CommAddress>(knownPeersVector_);
  }

  @Override
  public synchronized void addFoundPeer(CommAddress address) {
    // TODO(mbw): address != null, because of Broker.java:40
    if (!knownPeers_.contains(address) && address != null) {
      logger_.debug("Adding a CommAddress: " + address);
      knownPeers_.add(address);
      knownPeersVector_.add(address);

      if (randomPeersSample_.size() < RandomPeersGossipingModule.RANDOM_PEERS_SAMPLE_SIZE) {
        randomPeersSample_.add(address);
      }
      contextChanged();
    }
  }

  @Override
  public Set<CommAddress> getRandomPeersSample() {
    return randomPeersSample_;
  }

  @Override
  public void setRandomPeersSample(Set<CommAddress> randomPeersSample) {
    logger_.debug("Set random peers sample size: " + randomPeersSample.size() + " was: " +
        randomPeersSample_.size());
    randomPeersSample_ = randomPeersSample;
  }

  /**
   * Visitor.
   */
  public class NetworkMonitorVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Initialized...");
      timer_.scheduleRepeatedJob(randomPeersGossipingModuleProvider_,
          statisticsUpdateIntervalMillis_, statisticsUpdateIntervalMillis_);
      return null;
    }

    public Void visit(ConnectionTestMessage message) {
      logger_.debug("Got ConnectionTestMessage.");
      ConnectionTestMessageHandler handler = connectionTestMessageHandlerProvider_.get();
      message.setHandler(handler);
      dispatcherQueue_.add(message);
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
