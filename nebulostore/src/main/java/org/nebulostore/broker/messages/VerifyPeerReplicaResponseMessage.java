package org.nebulostore.broker.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.replicator.messages.QueryToStoreObjectMessage;

public class VerifyPeerReplicaResponseMessage extends Message {

  private static final long serialVersionUID = -4817614840279629913L;
  private boolean decision_;
  private QueryToStoreObjectMessage message_;

  public VerifyPeerReplicaResponseMessage(String jobId, boolean decision,
      QueryToStoreObjectMessage message) {
    super(jobId);
    decision_ = decision;
    message_ = message;
  }

  public boolean getDecision() {
    return decision_;
  }

  public QueryToStoreObjectMessage getVerifyiedMessage() {
    return message_;
  };
}
