package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.MessageMatcher;

/**
 * Initial ping message used in message exchange test.
 *
 * @author Grzegorz Milka
 *
 */
public class PingPongMessage extends CommMessage {
  private static final long serialVersionUID = 1L;
  private static final MessageMatcher PING_MESSAGE_MATCHER = new PingPongMessageMatcher(Type.PING);
  private static final MessageMatcher PONG_MESSAGE_MATCHER = new PingPongMessageMatcher(Type.PONG);
  private final Type type_;

  public PingPongMessage(CommAddress sourceAddress, CommAddress destAddress, Type type) {
    super(sourceAddress, destAddress);
    type_ = type;
  }

  public static MessageMatcher getMessageMatcher(Type type) {
    if (type.equals(Type.PING)) {
      return PING_MESSAGE_MATCHER;
    } else {
      return PONG_MESSAGE_MATCHER;
    }
  }

  public Type getType() {
    return type_;
  }

  public String toString() {
    return String.format("PingPongMessage[%s]", type_ == Type.PING ? "PING" : "PONG");
  }

  /**
   * @author Grzegorz Milka
   */
  public enum Type {
    PING,
    PONG
  };

  /**
   * @author Grzegorz Milka
   */
  private static class PingPongMessageMatcher implements MessageMatcher {
    private final Type type_;

    public PingPongMessageMatcher(Type type) {
      type_ = type;
    }

    @Override
    public boolean matchMessage(CommMessage msg) {
      if (msg instanceof PingPongMessage && ((PingPongMessage) msg).getType().equals(type_)) {
        return true;
      }
      return false;
    }
  }
}
