package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Request to delete a particular object from a peer that is replicating it.
 * @author Bolek Kulbabinski
 */
public class DeleteObjectMessage extends InReplicatorMessage {
  private static final long serialVersionUID = -587693375265935213L;

  private ObjectId objectId_;
  private final String sourceJobId_;

  public DeleteObjectMessage(String jobId, CommAddress destAddress, ObjectId objectId,
      String sourceJobId) {
    super(jobId, destAddress);
    sourceJobId_ = sourceJobId;
    objectId_ = objectId;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  public String getSourceJobId() {
    return sourceJobId_;
  }
}
