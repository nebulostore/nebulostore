package org.nebulostore.subscription.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.subscription.model.SubscriptionNotification;
import org.nebulostore.subscription.modules.SubscriptionReceivedModule;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: rafalhryciuk.
 */
public class NotifySubscriberMessage extends CommMessage {
  private static final long serialVersionUID = 7033610614241379578L;

  private SubscriptionNotification subscriptionNotification_;


  public NotifySubscriberMessage(CommAddress sourceAddress, CommAddress destAddress,
                                 SubscriptionNotification subscriptionNotification) {
    super(sourceAddress, destAddress);
    init(subscriptionNotification);
  }

  public NotifySubscriberMessage(String jobId, CommAddress sourceAddress,
                                 CommAddress destAddress,
                                 SubscriptionNotification subscriptionNotification) {
    super(jobId, sourceAddress, destAddress);
    init(subscriptionNotification);
  }

  private void init(SubscriptionNotification subscriptionNotification) {
    this.subscriptionNotification_ = checkNotNull(subscriptionNotification);
  }

  public SubscriptionNotification getSubscriptionNotification() {
    return subscriptionNotification_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new SubscriptionReceivedModule();
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
