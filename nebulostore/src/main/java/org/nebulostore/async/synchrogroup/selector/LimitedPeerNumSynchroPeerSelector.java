package org.nebulostore.async.synchrogroup.selector;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

public class LimitedPeerNumSynchroPeerSelector implements SynchroPeerSelector {

  private static final long MAX_SYNCHRO_PEERS_NUM = 5;

  private AsyncMessagesContext context_;
  private CommAddress myAddress_;

  @Inject
  public void setDependencies(AsyncMessagesContext context, CommAddress myAddress) {
    context_ = context;
    myAddress_ = myAddress;
  }

  @Override
  public Pair<Set<CommAddress>, Set<CommAddress>> decide(CommAddress newPeer) {
    Set<CommAddress> peersToAdd = null;
    if (context_.getSynchroGroupForPeerCopy(myAddress_).size() < MAX_SYNCHRO_PEERS_NUM) {
      peersToAdd = Sets.newHashSet(newPeer);
    }
    return new Pair<Set<CommAddress>, Set<CommAddress>>(peersToAdd, null);
  }

}
