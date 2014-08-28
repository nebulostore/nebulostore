package org.nebulostore.benchmarks.labtest;

import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;

public class SimpleActivePeerConfiguration extends PeerConfiguration {

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(SimpleActivePeer.class);
  }
}
