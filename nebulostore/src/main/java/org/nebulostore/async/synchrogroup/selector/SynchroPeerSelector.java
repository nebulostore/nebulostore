package org.nebulostore.async.synchrogroup.selector;

import java.util.Set;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

/**
 * Interface for synchro-peer selectors. Instances of each class that implements it should decide if
 * new peer should be added as synchro-peer and prepare any other necessary changes to synchro-peer
 * set.
 *
 * @author Piotr Malicki
 *
 */

public interface SynchroPeerSelector {

  /**
   * This method should decide how to update synchro-peer set of current peer when newPeer is
   * discovered. It should return a pair of sets of CommAddress objects. The first set should
   * contain peers that will be added to synchro-peer set and the second one should contain peers
   * that will be removed from synchro-peer set.
   *
   * @param newPeer
   *          last peer discovered by network monitor
   */
  Pair<Set<CommAddress>, Set<CommAddress>> decide(CommAddress newPeer);
}
