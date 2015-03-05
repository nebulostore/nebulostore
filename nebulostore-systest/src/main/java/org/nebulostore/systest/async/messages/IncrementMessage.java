package org.nebulostore.systest.async.messages;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.errorresponder.ErrorResponder;
import org.nebulostore.communication.routing.errorresponder.SendAsyncMessageErrorResponder;
import org.nebulostore.systest.async.CounterModuleMessageForwarder;

/**
 * Message that is sent to Counter module of receiver in order to increment the
 * counter.
 *
 * @author Piotr Malicki
 *
 */
public class IncrementMessage extends CommMessage {

  private static Logger logger_ = Logger.getLogger(IncrementMessage.class);

  public IncrementMessage(CommAddress sourceAddress, CommAddress destAddress) {
    super(sourceAddress, destAddress);
  }

  private static final long serialVersionUID = -1368227037748745868L;

  @Override
  public ErrorResponder generateErrorResponder(BlockingQueue<Message> dispatcherQueue) {
    logger_.warn("Dispatcher queue: " + dispatcherQueue.hashCode());
    return new SendAsyncMessageErrorResponder(new AsynchronousIncrementMessage(),
        getDestinationAddress(), dispatcherQueue);
  }

  @Override
  public boolean requiresAck() {
    return true;
  }

  @Override
  public JobModule getHandler() {
    return new CounterModuleMessageForwarder(this);
  }
}
