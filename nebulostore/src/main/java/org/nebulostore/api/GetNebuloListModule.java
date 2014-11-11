package org.nebulostore.api;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.ListGetter;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.appcore.model.NebuloList;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.replicator.messages.GetListMessage;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;

import com.google.common.base.Predicate;

/**
 * Job module that fetches NebuloList or sublist of NebuloList (based on range/Predicate) from
 * NebuloStore.
 */
public class GetNebuloListModule extends GetModule<NebuloList> implements ListGetter {
  private static Logger logger_ = Logger.getLogger(GetNebuloListModule.class);
  private final GetNebuloListVisitor visitor_ = new GetNebuloListVisitor();

  private int fromIndex_;
  private int toIndex_;
  
  private Predicate<NebuloElement> predicate_;

  @Override
  public void fetchList(NebuloAddress address, CommAddress replicaAddress) {
    fetchList(address, -1, -1, null, replicaAddress);
  }

  @Override
  public void fetchList(NebuloAddress address, int fromIndex, int toIndex,
      CommAddress replicaAddress) {
    fetchList(address, fromIndex, toIndex, null, replicaAddress);
  }

  @Override
  public void fetchList(NebuloAddress address, Predicate<NebuloElement> predicate,
      CommAddress replicaAddress) {
    fetchList(address, -1, -1, predicate, replicaAddress);
  }

  @Override
  public void fetchList(NebuloAddress address, int fromIndex, int toIndex,
      Predicate<NebuloElement> predicate, CommAddress replicaAddress) {
    fromIndex_ = fromIndex;
    toIndex_ = toIndex;
    predicate_ = predicate;
    fetchObject(address, replicaAddress);
  }

  /**
   * Visitor.
   */
  protected class GetNebuloListVisitor extends GetModuleVisitor {

    @Override
    public Void visit(SendObjectMessage message) {
      if (state_ == STATE.REPLICA_FETCH) {

        NebuloList nebuloList;
        try {
          // TODO Just deserialize
          nebuloList = (NebuloList) CryptoUtils.decryptObject(message.getEncryptedEntity());
          nebuloList.setSender(message.getSourceAddress());
          nebuloList.setVersions(message.getVersions());
        } catch (CryptoException exception) {
          endWithError(exception);
          return null;
        }

        state_ = STATE.FILE_RECEIVED;
        endWithSuccess(nebuloList);

      } else {
        logger_.warn("SendObjectMessage received in state " + state_);
      }
      return null;
    }

    @Override
    protected GetObjectMessage prepareGetMessage(CommAddress replicator) {
      return new GetListMessage(CryptoUtils.getRandomId().toString(), replicator,
          address_.getObjectId(), jobId_, fromIndex_, toIndex_, predicate_);
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  @Override
  public NebuloList awaitResult(int timeoutSec) throws NebuloException {
    return getResult(timeoutSec);
  }

}