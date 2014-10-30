package org.nebulostore.replicator.messages;

import java.util.List;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.communication.naming.CommAddress;

/**
 * This message is a request for appending a series of elements to the end of NebuloList, the
 * replicator is storing, according to the 'append protocol'. If the set of replicators, replicating
 * this list is sent along with the data, receiver is in charge of propagating appnd to the rest of
 * replicators.
 */
public class AppendElementsMessage extends InReplicatorMessage {
  private static final long serialVersionUID = 1077956286951592789L;

  private final ObjectId listId_;
  private final List<NebuloElement> elementsToAppend_;
  private final ReplicationGroup replicators_;
  private final String sourceJobId_;

  // TODO version, epoch

  public AppendElementsMessage(CommAddress destAddress, ObjectId listId,
      List<NebuloElement> elementsToAppend, String sourceJobId) {
    super(destAddress);
    listId_ = listId;
    elementsToAppend_ = elementsToAppend;
    replicators_ = null;
    sourceJobId_ = sourceJobId;
  }

  public AppendElementsMessage(CommAddress destAddress, ObjectId listId,
      List<NebuloElement> elementsToAppend, ReplicationGroup replicators, String sourceJobId) {
    super(destAddress);
    listId_ = listId;
    elementsToAppend_ = elementsToAppend;
    replicators_ = replicators;
    sourceJobId_ = sourceJobId;
  }

  public ObjectId getListId() {
    return listId_;
  }

  public List<NebuloElement> getElementsToAppend() {
    return elementsToAppend_;
  }

  public ReplicationGroup getReplicators() {
    return replicators_;
  }

  public boolean shouldPropagate() {
    return replicators_ != null;
  }

  public String getSourceJobId() {
    return sourceJobId_;
  }
}
