package org.nebulostore.systest.performance;

import org.nebulostore.communication.gossip.GossipService;
import org.nebulostore.communication.gossip.OneTimeUniformGossipService;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.systest.TestingPeerConfiguration;

/**
 * Configuration for PerfTestingPeer.
 * @author Bolek Kulbabinski
 */
public class PerfTestingPeerConfiguration extends TestingPeerConfiguration {
  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(PerfTestingPeer.class);
  }

  @Override
  protected void configureCommunicationPeer() {
    bind(GossipService.class).to(OneTimeUniformGossipService.class);
  }
}
