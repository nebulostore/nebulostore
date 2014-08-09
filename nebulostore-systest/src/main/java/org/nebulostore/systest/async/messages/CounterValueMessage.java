package org.nebulostore.systest.async.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message with current value of counter in CounterModule.
 *
 * @author Piotr Malicki
 *
 */
public class CounterValueMessage extends Message {

  private static final long serialVersionUID = 2829507065421964890L;

  private final int value_;

  public CounterValueMessage(String jobId, int value) {
    super(jobId);
    value_ = value;
  }

  public int getValue() {
    return value_;
  }

}
