package org.nebulostore.api;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.replicator.messages.SendObjectMessage;

/**
 * Job module that fetches an existing object from NebuloStore.
 *
 * @author Bolek Kulbabinski
 */
public class GetNebuloObjectModule extends
    GetFullObjectModule<NebuloObject> implements ObjectGetter {
  private final StateMachineVisitor visitor_ = new StateMachineVisitor();

  @Inject
  public GetNebuloObjectModule(ObjectRecreator recreator) {
    recreator_ = recreator;
  }

  @Override
  public NebuloObject awaitResult(int timeoutSec) throws NebuloException {
    return getResult(timeoutSec);
  }

  protected class StateMachineVisitor extends GetFullObjectModuleVisitor {

    @Override
    public void visit(SendObjectMessage message) {
      EncryptedObject fullObject;
      try {
        fullObject = tryRecreateFullObject(message);
      } catch (NebuloException e) {
        endWithError(e);
        return;
      }

      if (fullObject != null) {
        NebuloObject nebuloObject;
        try {
          nebuloObject = (NebuloObject) decryptWrapper_.decrypt(fullObject);
          String ownerPublicKey = networkMonitor_.getUserPublicKeyId(
              nebuloObject.getAddress().getAppKey());
          String version = currentVersions_.get(currentVersions_.size() - 1);
          if (!encryption_.verifyMAC(nebuloObject, version, ownerPublicKey)) {
            throw new NebuloException("Verify version error");
          }
          nebuloObject.setVersions(currentVersions_);
        } catch (NebuloException exception) {
          // TODO(bolek): Error not fatal? Retry?
          endWithError(exception);
          return;
        }

        // State 3 - Finally got the file, return it;
        state_ = STATE.FILE_RECEIVED;
        endWithSuccess(nebuloObject);
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
