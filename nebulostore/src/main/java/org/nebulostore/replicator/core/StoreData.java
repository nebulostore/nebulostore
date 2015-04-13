package org.nebulostore.replicator.core;

import java.io.Serializable;
import java.util.Set;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class StoreData implements Serializable {

  private static final long serialVersionUID = 3493488149204782292L;

  private String remoteJobId_;
  private ObjectId objectId_;
  private EncryptedObject data_;
  private Set<String> previousVersionSHAs_;

  public StoreData(String remoteJobId, ObjectId objectId, EncryptedObject data,
      Set<String> previousVersionSHAs) {
    remoteJobId_ = remoteJobId;
    objectId_ = objectId;
    data_ = data;
    previousVersionSHAs_ = previousVersionSHAs;
  }

  public String getRemoteJobId() {
    return remoteJobId_;
  }

  public ObjectId getObjectId() {
    return objectId_;
  }

  public EncryptedObject getData() {
    return data_;
  }

  public Set<String> getPreviousVersionSHAs() {
    return previousVersionSHAs_;
  }

}
