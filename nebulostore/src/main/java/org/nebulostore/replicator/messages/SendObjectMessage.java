package org.nebulostore.replicator.messages;

import java.util.Set;

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

  private final Set<String> versions_;

  public SendObjectMessage(CommAddress destAddress, EncryptedObject encryptedObject,
      Set<String> versions) {
    super(destAddress);
    encryptedEntity_ = encryptedObject;
    Cloner c = new Cloner();
    versions_ = c.deepClone(versions);
  }

  public SendObjectMessage(String jobId, CommAddress destAddress, EncryptedObject encryptedObject,
      Set<String> versions) {
    super(jobId, destAddress);
    encryptedEntity_ = encryptedObject;
    Cloner c = new Cloner();
    versions_ = c.deepClone(versions);
  }

  public EncryptedObject getEncryptedEntity() {
    return encryptedEntity_;
  }

  public Set<String> getVersions() {
    return versions_;
  }
}
