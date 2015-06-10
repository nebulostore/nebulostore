package org.nebulostore.api.acl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.model.EncryptedObject;

/**
 * @author lukaszsiczek
 */
public class ACLAccessData implements Serializable {

  private static final long serialVersionUID = 494583199482327780L;

  private Map<AppKey, EncryptedObject> accessMap_ = new HashMap<AppKey, EncryptedObject>();

  public void add(AppKey appKey, EncryptedObject encryptedObject) {
    accessMap_.put(appKey, encryptedObject);
  }

  public EncryptedObject get(AppKey appKey) {
    return accessMap_.get(appKey);
  }

}
