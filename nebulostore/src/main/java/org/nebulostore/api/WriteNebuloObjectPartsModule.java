package org.nebulostore.api;

import java.util.List;
import java.util.Map;

import com.google.inject.Inject;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.PartialObjectWriter;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dispatcher.JobInitMessage;

public class WriteNebuloObjectPartsModule extends WriteModule implements PartialObjectWriter {

  private Map<CommAddress, EncryptedObject> objectsMap_;
  private List<String> previousVersionSHAs_;
  private ObjectId objectId_;

  @Inject
  public WriteNebuloObjectPartsModule(EncryptionAPI encryption) {
    super(encryption);
  }

  @Override
  protected WriteModuleVisitor createVisitor() {
    return new WriteNebuloObjectPartsVisitor();
  }

  @Override
  public void writeObject(Map<CommAddress, EncryptedObject> objectsMap,
      List<String> previousVersionsSHAs, int confirmationsRequired, ObjectId objectId) {
    objectsMap_ = objectsMap;
    previousVersionSHAs_ = previousVersionsSHAs;
    objectId_ = objectId;
    super.writeObject(confirmationsRequired);
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  protected class WriteNebuloObjectPartsVisitor extends WriteModuleVisitor {

    public void visit(JobInitMessage message) {
      sendStoreQueries(objectsMap_, previousVersionSHAs_, true, null, objectId_);
    }
  }


}
