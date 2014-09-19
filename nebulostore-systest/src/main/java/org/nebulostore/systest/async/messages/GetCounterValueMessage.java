package org.nebulostore.systest.async.messages;

import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.messaging.Message;

public class GetCounterValueMessage extends Message {

  private static final long serialVersionUID = -4218151067718102546L;

  private final BlockingQueue<Message> senderInQueue_;
  private final String senderJobId_;

  public GetCounterValueMessage(BlockingQueue<Message> senderInQueue, String senderJobId) {
    senderInQueue_ = senderInQueue;
    senderJobId_ = senderJobId;
  }

  public BlockingQueue<Message> getSenderInQeue() {
    return senderInQueue_;
  }

  public String getSenderJobId() {
    return senderJobId_;
  }

}
