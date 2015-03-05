package org.nebulostore.subscription.modules;

import com.google.inject.Inject;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.subscription.api.SubscriptionNotificationHandler;
import org.nebulostore.subscription.messages.NotifySubscriberMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: rafalhryciuk.
 */
public class SubscriptionReceivedModule extends JobModule {

  private SubscriptionNotificationHandler notificationHandler_;

  private final SubscriptionReceivedMessageVisitor visitor_ =
      new SubscriptionReceivedMessageVisitor();


  public SubscriptionReceivedModule() {
  }

  @Inject
  public void setNotificationHandler(SubscriptionNotificationHandler notificationHandler) {
    this.notificationHandler_ = checkNotNull(notificationHandler);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Message handler for received subscription notification.
   */
  protected class SubscriptionReceivedMessageVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) throws NebuloException {
    }

    public void visit(NotifySubscriberMessage message) throws NebuloException {
      notificationHandler_.handleSubscriptionNotification(message.getSubscriptionNotification());
      endJobModule();
    }
  }
}
