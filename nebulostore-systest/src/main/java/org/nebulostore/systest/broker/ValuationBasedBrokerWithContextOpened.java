package org.nebulostore.systest.broker;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.broker.ValuationBasedBroker;
import org.nebulostore.systest.broker.messages.BrokerContextMessage;
import org.nebulostore.systest.broker.messages.GetBrokerContextMessage;

/**
 * Valuation based broker that allows to retrieve its context by sending GetBrokerContextMessage.
 *
 * @author szymonmatejczyk
 *
 */
public class ValuationBasedBrokerWithContextOpened extends ValuationBasedBroker {
  private static Logger logger_ = Logger.getLogger(ValuationBasedBrokerWithContextOpened.class);
  public final BrokerVisitor visitor_ = new ThisVisitor();

  /**
   * Visitor.
   */
  public class ThisVisitor extends BrokerVisitor {
    public void visit(GetBrokerContextMessage message) {
      logger_.debug("Got GetBrokerContextMessage.");
      outQueue_.add(new BrokerContextMessage(message.getId(), context_));
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
