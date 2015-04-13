package org.nebulostore.replicator.messages;

import java.util.Set;

import com.rits.cloning.Cloner;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.SessionInnerMessageInterface;

/**
 * @author szymonmatejczyk
 * This is a query to store a particular object.
 */
public class QueryToStoreObjectMessage extends InReplicatorMessage
    implements SessionInnerMessageInterface {
  private static final long serialVersionUID = 3283983404037381657L;

  ObjectId objectId_;
  EncryptedObject encryptedEntity_;
  Set<String> previousVersionSHAs_;
  private final String sourceJobId_;
  private String sessionId_;

  public QueryToStoreObjectMessage(String jobId,
      CommAddress destAddress, ObjectId objectId, EncryptedObject encryptedEntity,
      Set<String> previousVersionSHAs, String sourceJobId, String sessionId) {
    super(jobId, destAddress);
    objectId_ = objectId;
    encryptedEntity_ = encryptedEntity;
    Cloner c = new Cloner();
    previousVersionSHAs_ = c.deepClone(previousVersionSHAs);
    sourceJobId_ = sourceJobId;
    sessionId_ = sessionId;
  }

  public EncryptedObject getEncryptedEntity() {
    return encryptedEntity_;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  @Override
  public String getSourceJobId() {
    return sourceJobId_;
  }

  public Set<String> getPreviousVersionSHAs() {
    return previousVersionSHAs_;
  }

  public String getSessionId() {
    return sessionId_;
  }
}
