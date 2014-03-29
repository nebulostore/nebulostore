package org.nebulostore.networkmonitor;

import java.util.List;
import java.util.Set;

import com.google.inject.Inject;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.timer.MessageGenerator;

/**
 * Singleton module that monitors other peers in the network and adds statistics to DHT.
 * @author szymon
 *
 */
public abstract class NetworkMonitor extends JobModule {
  protected CommAddress myAddress_;

  public abstract void addContextChangeMessageGenerator(MessageGenerator generator);

  public abstract void removeContextChangeMessageGenerator(MessageGenerator generator);

  public abstract List<CommAddress> getKnownPeers();

  public abstract void addFoundPeer(CommAddress address);

  public abstract Set<CommAddress> getRandomPeersSample();

  public abstract void setRandomPeersSample(Set<CommAddress> randomPeersSample);

  @Inject
  private void setDependencies(CommAddress commAddress) {
    myAddress_ = commAddress;
  }

  public double getResponseFrequency() {
    throw new UnsupportedOperationException();
  }
}
