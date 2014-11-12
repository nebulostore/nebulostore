package org.nebulostore.async.synchrogroup.selector;

import java.util.Set;

import com.google.common.collect.Sets;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

/**
 * Synchro-peer selector that accepts all discovered peers as new synchro-peers.
 *
 * @author Piotr Malicki
 *
 */
public class AlwaysAcceptingSynchroPeerSelector implements SynchroPeerSelector {

  @Override
  public Pair<Set<CommAddress>, Set<CommAddress>> decide(CommAddress newPeer) {
    return new Pair<Set<CommAddress>, Set<CommAddress>>(Sets.newHashSet(newPeer), null);
  }

}
