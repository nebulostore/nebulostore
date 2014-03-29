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
public class UpdateNebuloObjectMessage extends AsynchronousMessage {
  private static final long serialVersionUID = 1428811392987901652L;

  NebuloAddress objectId_;

  /* It is assumed that instance of Nebulostore has persistent CommAddress. */
  CommAddress updateFrom_;

  public UpdateNebuloObjectMessage(NebuloAddress objectId, CommAddress updateFrom) {
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

  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
