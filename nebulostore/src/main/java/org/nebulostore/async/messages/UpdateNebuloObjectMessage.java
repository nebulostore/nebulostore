package org.nebulostore.async.messages;

import org.nebulostore.appcore.addressing.NebuloAddress;
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
