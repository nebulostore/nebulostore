package org.nebulostore.replicator.messages;

import java.util.List;

import com.rits.cloning.Cloner;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * This is a message containing requested object.
 * @author Bolek Kulbabinski
 */
public class SendObjectMessage extends OutReplicatorMessage {
  private static final long serialVersionUID = 5852937000391705084L;

  private final EncryptedObject encryptedEntity_;
  private String sessionId_;

  private final List<String> versions_;

  public SendObjectMessage(CommAddress destAddress, EncryptedObject encryptedObject,
      List<String> versions) {
    super(destAddress);
    encryptedEntity_ = encryptedObject;
    Cloner c = new Cloner();
    versions_ = c.deepClone(versions);
  }

  public SendObjectMessage(String jobId, CommAddress destAddress, String sessionId,
      EncryptedObject encryptedObject, List<String> versions) {
    super(jobId, destAddress);
    encryptedEntity_ = encryptedObject;
    sessionId_ = sessionId;
    Cloner c = new Cloner();
    versions_ = c.deepClone(versions);
  }

  public EncryptedObject getEncryptedEntity() {
    return encryptedEntity_;
  }

  public List<String> getVersions() {
    return versions_;
  }

  public String getSessionId() {
    return sessionId_;
  }
}
