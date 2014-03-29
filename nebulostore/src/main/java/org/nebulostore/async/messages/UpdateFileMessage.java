package org.nebulostore.async.messages;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Message send to peer when he needs to update file with @objectId,
 * that he stores, from @updateFrom.
 * @author szymonmatejczyk
 */
public class UpdateFileMessage extends AsynchronousMessage {
  private static final long serialVersionUID = -4412275517980056063L;

  NebuloAddress objectId_;

  /* It is assumed that instance of Nebulostore has persistent CommAddress. */
  CommAddress updateFrom_;

  public UpdateFileMessage(NebuloAddress objectId, CommAddress updateFrom) {
    super();
    objectId_ = objectId;
    updateFrom_ = updateFrom;
  }

  public NebuloAddress getObjectId() {
    return objectId_;
  }

  public CommAddress getUpdateFrom() {
    return updateFrom_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
