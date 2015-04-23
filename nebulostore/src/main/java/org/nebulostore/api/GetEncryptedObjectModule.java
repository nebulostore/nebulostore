package org.nebulostore.api;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.utils.Pair;

/**
 * @author szymonmatejczyk
 */
public class GetEncryptedObjectModule extends
    GetFullObjectModule<Pair<EncryptedObject, List<String>>> {
  private final GetEncryptedObjectVisitor visitor_ = new GetEncryptedObjectVisitor();

  public GetEncryptedObjectModule(NebuloAddress nebuloAddress) {
    fetchObject(nebuloAddress);
  }

  /**
   * Constructor that runs this module through dispatcher.
   */
  public GetEncryptedObjectModule(NebuloAddress nebuloAddress,
      BlockingQueue<Message> dispatcherQueue) {
    setDispatcherQueue(dispatcherQueue);
    fetchObject(nebuloAddress);
  }


  protected class GetEncryptedObjectVisitor extends GetFullObjectModuleVisitor {

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
        endWithSuccess(new Pair<EncryptedObject, List<String>>(fullObject, message.getVersions()));
      }
    }
  }


  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
