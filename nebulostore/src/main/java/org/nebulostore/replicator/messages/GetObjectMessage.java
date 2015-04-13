package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;

/**
 * This is a request for a particular object sent to peer that hold this
 * object's replica. Sender waits for a response wrapped into SendObjectMessage.
 * @author Bolek Kulbabinski
 */
public class GetObjectMessage extends InReplicatorMessage implements SessionInnerMessageInterface {
  private static final long serialVersionUID = 1660420694986822395L;

  private final ObjectId objectId_;
  private final String sourceJobId_;
  private String sessionId_;

  public ObjectId getObjectId() {
    return objectId_;
  }

  public GetObjectMessage(CommAddress destAddress, ObjectId objectId, String sourceJobId) {
    super(destAddress);
    objectId_ = objectId;
    sourceJobId_ = sourceJobId;
  }

  public GetObjectMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress,
      ObjectId objectId, String sourceJobId, String sessionId) {
    super(jobId, sourceAddress, destAddress);
    objectId_ = objectId;
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }

}
