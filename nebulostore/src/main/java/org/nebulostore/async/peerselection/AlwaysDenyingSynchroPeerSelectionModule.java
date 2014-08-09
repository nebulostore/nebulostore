package org.nebulostore.async.peerselection;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Simple synchro-peer selection module that ignores all given peers.
 *
 * @author Piotr Malicki
 *
 */
public class AlwaysDenyingSynchroPeerSelectionModule extends SynchroPeerSelectionModule {

  @Override
  protected void decide(CommAddress newPeer) {
  }

}
