package org.nebulostore.appcore.model;

import java.util.List;
import java.util.Map;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.communication.naming.CommAddress;

public interface PartialObjectWriter extends GeneralObjectWriter {

  void writeObject(Map<CommAddress, EncryptedObject> objectsMap, List<String> previousVersionSHAs,
      int confirmationsRequired, ObjectId objectId);

}
