package org.nebulostore.networkmonitor;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.networkmonitor.messages.ConnectionTestMessage;
import org.nebulostore.networkmonitor.messages.ConnectionTestResponseMessage;

/**
 * Responses to ConnectionTestMessage with ConnectionTestResponseMessage.
 *
 * @author szymon
 *
 */
public class DefaultConnectionTestMessageHandler extends ConnectionTestMessageHandler {
  protected CTMVisitor visitor_ = new CTMVisitor();

  public class CTMVisitor extends MessageVisitor {
    public void visit(ConnectionTestMessage message) {
      jobId_ = message.getId();
      networkQueue_.add(new ConnectionTestResponseMessage(message.getId(), message
          .getSourceAddress()));
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
