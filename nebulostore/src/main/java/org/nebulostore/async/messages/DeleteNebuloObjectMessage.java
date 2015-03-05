package org.nebulostore.async.messages;

import org.nebulostore.appcore.addressing.NebuloAddress;

/**
 * Message sent to peer when a NebuloObject with @objectId_ was deleted.
 * @author bolek
 */
public class DeleteNebuloObjectMessage extends AsynchronousMessage {
  private static final long serialVersionUID = -7801787656898849915L;

  NebuloAddress objectId_;

  public DeleteNebuloObjectMessage(NebuloAddress objectId) {
    objectId_ = objectId;
  }

  public NebuloAddress getObjectId() {
    return objectId_;
  }
}
