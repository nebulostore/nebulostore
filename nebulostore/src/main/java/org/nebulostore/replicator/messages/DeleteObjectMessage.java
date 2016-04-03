package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;

/**
 * Request to delete a particular object from a peer that is replicating it.
 * @author Bolek Kulbabinski
 */
public class DeleteObjectMessage extends InReplicatorMessage
    implements SessionInnerMessageInterface {
  private static final long serialVersionUID = -587693375265935213L;

  private EncryptedObject encryptedData_;
  private final String sourceJobId_;
  private String sessionId_;

  public DeleteObjectMessage(String jobId, CommAddress destAddress, EncryptedObject encryptedData,
      String sourceJobId, String sessionId) {
    super(jobId, destAddress);
    sourceJobId_ = sourceJobId;
    encryptedData_ = encryptedData;
    sessionId_ = sessionId;
  }

  public EncryptedObject getEncryptedData() {
    return encryptedData_;
  }

  public String getSourceJobId() {
    return sourceJobId_;
  }

  public String getSessionId() {
    return sessionId_;
  }
}
