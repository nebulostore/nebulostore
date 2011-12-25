package org.nebulostore.dispatcher.messages;

import org.nebulostore.appcore.Message;
import org.nebulostore.appcore.MessageVisitor;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Worker thread sends this message to dispatcher before it dies to
 * indicate that the task has ended and can be removed from the thread map.
 *
 */
public class JobEndedMessage extends Message {
  public JobEndedMessage(String msgID) {
    super(msgID);
  }
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
