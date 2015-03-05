package org.nebulostore.networkmonitor.messages;

import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.RandomPeersGossipingModule;

/**
 * Message containing set of random peers. Used by gossiping random peers protocol.

 * @author szymonmatejczyk
 */
public class RandomPeersSampleMessage extends CommMessage {

  private Provider<RandomPeersGossipingModule> provider_;

  public RandomPeersSampleMessage(CommAddress destAddress, Set<CommAddress> peersSet) {
    super(null, destAddress);
    peersSet_ = new HashSet<CommAddress>(peersSet);
  }

  public RandomPeersSampleMessage(String jobId, CommAddress destAddress, Set<CommAddress> peersSet)
  {
    super(jobId, null, destAddress);
    peersSet_ = new HashSet<CommAddress>(peersSet);
  }

  @Inject
  public void setProvider(Provider<RandomPeersGossipingModule> provider) {
    provider_ = provider;
  }

  private static final long serialVersionUID = 7275803958168737858L;

  private final Set<CommAddress> peersSet_;

  /*
   * True if this is a response for previous RandomPeersSampleMessage.
   */
  public Set<CommAddress> getPeersSet() {
    return peersSet_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return provider_.get();
  }
}
