package org.nebulostore.replicator.core;

import java.io.Serializable;
import java.util.List;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class StoreData implements Serializable {

  private static final long serialVersionUID = 3493488149204782292L;

  private final String remoteJobId_;
  private final ObjectId objectId_;
  private final EncryptedObject data_;
  private final List<String> previousVersionSHAs_;
  private final String newVersionSHA_;

  public StoreData(String remoteJobId, ObjectId objectId, EncryptedObject data,
      List<String> previousVersionSHAs, String newVersionSHA) {
    remoteJobId_ = remoteJobId;
    objectId_ = objectId;
    data_ = data;
    previousVersionSHAs_ = previousVersionSHAs;
    newVersionSHA_ = newVersionSHA;
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

  public List<String> getPreviousVersionSHAs() {
    return previousVersionSHAs_;
  }

  public String getNewVersionSHA() {
    return newVersionSHA_;
  }

}
