package org.nebulostore.benchmarks.labtest;

import org.apache.log4j.Logger;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.peers.Peer;

public class SimpleActivePeer extends Peer {
  private static Logger logger_ = Logger.getLogger(SimpleActivePeer.class);
  
  @Override
  protected void runActively() {
    // TODO: Move register to a separate module or at least make it non-blocking.
    register(appKey_);
    logger_.debug("Successfully registered.");
    dispatcherInQueue_.add(new JobInitMessage(new RandomActivityModule()));
  }
}
