package org.nebulostore.communication.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.PeerFoundHandler;

/**
 * Notifies about found peers in the network.
 * Contains CommAddress of the newly found peer in sourceAdress field.
 * destAddress should be current peer's address. This message is to be used
 * internally (not sent to other peers).
 * @author Grzegorz Milka
 * @author Marcin Walas
 */
public class CommPeerFoundMessage extends CommMessage {
  private static final long serialVersionUID = -8231258035871545537L;

  public CommPeerFoundMessage(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);
  }

  public CommPeerFoundMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
  }

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new PeerFoundHandler();
  }

  @Override
  public String toString() {
    return "CommPeerFoundMessage about peer: " + getSourceAddress();
  }
}
