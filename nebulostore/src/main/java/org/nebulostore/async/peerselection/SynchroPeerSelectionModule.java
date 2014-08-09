package org.nebulostore.async.peerselection;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * Base class for synchro-peer selection modules. Each subclass should check if new peer should be
 * added as synchro-peer and make any other necessary changes to synchro-peer set. This module
 * should run other modules that will update synchro-peer set.
 *
 * @author Piotr Malicki
 *
 */
public abstract class SynchroPeerSelectionModule extends JobModule {

  private static final Logger LOGGER = Logger.getLogger(SynchroPeerSelectionModule.class);

  protected AsyncMessagesContext context_;
  private final MessageVisitor<Void> visitor_;

  private NetworkMonitor networkMonitor_;

  public SynchroPeerSelectionModule() {
    visitor_ = new SPSVisitor();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  @Inject
  public void setContext(final AsyncMessagesContext context) {
    context_ = context;
  }

  @Inject
  public void setNetworkMonitor(final NetworkMonitor networkMonitor) {
    networkMonitor_ = networkMonitor;
  }

  /**
   * This method should decide how to update synchro-peer set of current peer when newPeer is
   * discovered. It should run modules that will update async messages context properly.
   *
   * @param newPeer
   *          last peer discovered by network monitor
   */
  protected abstract void decide(CommAddress newPeer);

  protected class SPSVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      int lastSynchroPeerIndex = networkMonitor_.getKnownPeers().size() - 1;
      CommAddress synchroPeer = networkMonitor_.getKnownPeers().get(lastSynchroPeerIndex);
      if (synchroPeer == null) {
        LOGGER.warn("Empy synchro peer got as the last from NetworkContext.");
        endJobModule();
        return null;
      }

      decide(synchroPeer);
      endJobModule();
      return null;
    }
  }

}
