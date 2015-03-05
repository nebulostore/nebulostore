package org.nebulostore.systest.async;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.systest.async.messages.AsynchronousIncrementMessage;
import org.nebulostore.systest.async.messages.CounterValueMessage;
import org.nebulostore.systest.async.messages.GetCounterValueMessage;

/**
 * Simple module that counts the number of received messages of type AsynchronousIncrementMessage.
 *
 * @author Piotr Malicki
 *
 */
public class CounterModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(CounterModule.class);

  private int number_;
  private final MessageVisitor visitor_ = new CounterModuleMessageVisitor();

  public int getNumber() {
    return number_;
  }

  protected class CounterModuleMessageVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
    }

    public void visit(GetCounterValueMessage message) {
      outQueue_.add(new CounterValueMessage(message.getSenderJobId(), number_));
    }

    public void visit(AsynchronousIncrementMessage message) {
      number_++;
    }

    public void visit(EndModuleMessage message) {
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    logger_.debug("Received message in counter module: " + message);
    message.accept(visitor_);
  }

}
