package org.nebulostore.async.messages;

import org.nebulostore.appcore.addressing.NebuloAddress;
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
    objectId_ = objectId;
    updateFrom_ = updateFrom;
  }

  public NebuloAddress getObjectId() {
    return objectId_;
  }

  public CommAddress getUpdateFrom() {
    return updateFrom_;
  }

}
