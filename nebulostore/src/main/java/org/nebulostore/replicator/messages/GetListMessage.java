package org.nebulostore.replicator.messages;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.utils.Pair;

import com.google.common.base.Predicate;

public class GetListMessage extends GetObjectMessage {
  private static final long serialVersionUID = -4172307854002333547L;

  private final int startIndex_;
  private final int endIndex_;
  
  private final Predicate<NebuloElement> predicate_;

  public GetListMessage(CommAddress destAddress, ObjectId objectId, String sourceJobId,
      int startRange, int endRange, Predicate<NebuloElement> predicate) {
    super(destAddress, objectId, sourceJobId);
    startIndex_ = startRange;
    endIndex_ = endRange;
    predicate_ = predicate;
  }

  public GetListMessage(String jobId, CommAddress destAddress, ObjectId objectId,
      String sourceJobId, int startRange, int endRange, Predicate<NebuloElement> predicate) {
    super(jobId, destAddress, objectId, sourceJobId);
    startIndex_ = startRange;
    endIndex_ = endRange;
    predicate_ = predicate;
  }

  public boolean hasRange() {
    return endIndex_ >= 0;
  }

  public boolean hasPredicate() {
    return predicate_ != null;
  }

  public Pair<Integer, Integer> getRange() {
    return new Pair<Integer, Integer>(startIndex_, endIndex_);
  }

  public Predicate<NebuloElement> getPredicate() {
    return predicate_;
  }
}
