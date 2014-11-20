package org.nebulostore.async.checker.messages;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.checker.MessageReceivingCheckerMessageForwarder;

/**
 * Message sent to the message receiving checker module periodically indicating that next time
 * period elapsed.
 *
 * @author Piotr Malicki.
 *
 */
public class TickMessage extends Message {

  private static final long serialVersionUID = 5810748984861758414L;

  @Override
  public JobModule getHandler() {
    return new MessageReceivingCheckerMessageForwarder(this);
  }

}
