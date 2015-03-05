package org.nebulostore.networkmonitor;
import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommPeerFoundMessage;

/**
 * Module handle CommPeerFoundMessage.
 * @author szymonmatejczyk
 */
public class PeerFoundHandler extends JobModule {

  private final PeerFoundHandlerVisitor visitor_ = new PeerFoundHandlerVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private NetworkMonitor networkMonitor_;

  @Inject
  public void setDependencies(NetworkMonitor networkMonitor) {
    networkMonitor_ = networkMonitor;
  }

  /**
   * @author szymonmatejczyk
   */
  protected class PeerFoundHandlerVisitor extends MessageVisitor {
    public void visit(CommPeerFoundMessage message) {
      jobId_ = message.getId();
      networkMonitor_.addFoundPeer(message.getSourceAddress());
      endJobModule();
    }
  }

}
