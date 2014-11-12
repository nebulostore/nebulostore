package org.nebulostore.async.synchrogroup.selector;

import java.util.Set;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

/**
 * Simple synchro-peer selector that ignores all given peers.
 *
 * @author Piotr Malicki
 *
 */
public class AlwaysDenyingSynchroPeerSelector implements SynchroPeerSelector {

  @Override
  public Pair<Set<CommAddress>, Set<CommAddress>> decide(CommAddress newPeer) {
    return null;
  }

}
