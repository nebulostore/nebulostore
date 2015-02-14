package org.nebulostore.api;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author lukaszsiczek
 */
public class GetKeyModule extends ReturningJobModule<ValueDHT> {

  private static Logger logger_ = Logger.getLogger(GetKeyModule.class);
  private final MessageVisitor<Void> visitor_;
  private final KeyDHT keyDHT_;

  public GetKeyModule(BlockingQueue<Message> dispatcherQueue, KeyDHT keyDHT) {
    visitor_ = new GetKeyModuleMessageVisitor();
    keyDHT_ = keyDHT;
    setDispatcherQueue(checkNotNull(dispatcherQueue));
    runThroughDispatcher();
  }

  protected class GetKeyModuleMessageVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) {
      networkQueue_.add(new GetDHTMessage(getJobId(), keyDHT_));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      logger_.debug("Process ValueDHTMessage");
      endWithSuccess(message.getValue());
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.debug("Process ErrorDHTMessage");
      endWithError(message.getException());
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
