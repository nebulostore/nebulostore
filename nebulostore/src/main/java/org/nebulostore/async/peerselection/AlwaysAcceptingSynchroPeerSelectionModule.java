package org.nebulostore.async.peerselection;

import com.google.common.collect.Sets;

import org.nebulostore.async.ChangeSynchroPeerSetModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Synchro-peer selection module that adds all discovered peers to the context and DHT.
 *
 * @author Piotr Malicki
 *
 */
public class AlwaysAcceptingSynchroPeerSelectionModule extends SynchroPeerSelectionModule {

  @Override
  protected void decide(CommAddress newPeer) {
    outQueue_.add(new JobInitMessage(new ChangeSynchroPeerSetModule(
        Sets.newHashSet(newPeer), null)));
  }

}
