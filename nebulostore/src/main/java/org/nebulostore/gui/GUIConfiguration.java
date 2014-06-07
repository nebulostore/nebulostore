package org.nebulostore.gui;

import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.peers.PeerConfiguration;

public class GUIConfiguration extends PeerConfiguration {

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(GUIController.class);
  }
}
