package org.nebulostore.conductor.messages;

import java.util.Set;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author szymonmatejczyk
 */
public class ReconfigurationMessage extends CommMessage {

  private static final long serialVersionUID = -7407814653868855140L;
  private final Set<CommAddress> clients_;

  public ReconfigurationMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, Set<CommAddress> clients) {
    super(jobId, sourceAddress, destAddress);
    clients_ = clients;
  }

  public Set<CommAddress> getClients() {
    return clients_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
