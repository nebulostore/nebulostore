package org.nebulostore.communication.routing;

import org.nebulostore.communication.messages.CommMessage;

/**
 * Result of message send operation.
 *
 * @author Grzegorz Milka
 *
 */
public class SendResult {
  private final ResultType type_;
  private final CommMessage msg_;

  public SendResult(ResultType type, CommMessage msg) {
    type_ = type;
    msg_ = msg;
  }

  public ResultType getType() {
    return type_;
  }

  public CommMessage getMessage() {
    return msg_;
  }

  /**
   *
   * @author Grzegorz Milka
   *
   */
  public static enum ResultType {
    OK, ERROR;
  };
}
