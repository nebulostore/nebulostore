package org.nebulostore.subscription.modules;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.subscription.messages.NotifySubscriberMessage;
import org.nebulostore.subscription.model.SubscriptionNotification;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: rafalhryciuk.
 */
public class NotifySubscribersModule extends JobModule {

  private final CommAddress sourceAddress_;

  private final Set<CommAddress> subscribersAddresses_;

  private final SubscriptionNotification subscriptionNotification_;

  private final MessageVisitor<Void> visitor_ = new SubscriptionVisitor();


  public NotifySubscribersModule(CommAddress sourceAddress,
                                 BlockingQueue<Message> dispatcherQueue,
                                 SubscriptionNotification subscriptionNotification,
                                 Set<CommAddress> subscribersAddresses) {
    this.subscriptionNotification_ = checkNotNull(subscriptionNotification);
    this.subscribersAddresses_ = checkNotNull(subscribersAddresses);
    this.sourceAddress_ = checkNotNull(sourceAddress);
    this.outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Message handler for notifying subscribers about file change.
   */
  protected class SubscriptionVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) throws NebuloException {
      for (CommAddress subscriber : subscribersAddresses_) {
        if (!subscriber.equals(sourceAddress_)) {
          NotifySubscriberMessage subscribeMessage =
              new NotifySubscriberMessage(NotifySubscribersModule.this.getJobId(),
                  sourceAddress_, subscriber, subscriptionNotification_);
          networkQueue_.add(subscribeMessage);
        }
      }
      endJobModule();
      return null;
    }
  }
}
