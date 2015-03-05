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
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author lukaszsiczek
 */
public class PutKeyModule extends ReturningJobModule<Void> {

  private static Logger logger_ = Logger.getLogger(PutKeyModule.class);
  private final MessageVisitor visitor_;
  private final KeyDHT keyDHT_;
  private final ValueDHT valueDHT_;

  public PutKeyModule(BlockingQueue<Message> dispatcherQueue, KeyDHT keyDHT, ValueDHT valueDHT) {
    visitor_ = new PutKeyModuleMessageVisitor();
    keyDHT_ = keyDHT;
    valueDHT_ = valueDHT;
    setDispatcherQueue(checkNotNull(dispatcherQueue));
    runThroughDispatcher();
  }

  protected class PutKeyModuleMessageVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      networkQueue_.add(new PutDHTMessage(getJobId(), keyDHT_, valueDHT_));
    }

    public void visit(OkDHTMessage message) {
      logger_.debug("Successfully put key " + keyDHT_ + " in DHT");
      endWithSuccess(null);
    }

    public void visit(ErrorDHTMessage message) {
      endWithError(new NebuloException("DHT write returned with error", message.getException()));
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
