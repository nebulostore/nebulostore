package org.nebulostore.async;

import java.util.LinkedList;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.StoreAsynchronousMessage;

/**
 * Module responsible for storing asynchronous messages in this instance.
 *
 * @author szymonmatejczyk
 */
public class StoreAsynchronousMessagesModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(StoreAsynchronousMessagesModule.class);

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private AsyncMessagesContext context_;

  @Inject
  public void setDependencies(AsyncMessagesContext context) {
    context_ = context;
  }

  private final SAMVisitor visitor_ = new SAMVisitor();

  /**
   * Visitor.
   *
   * @author szymonmatejczyk
   * @author Piotr Malicki
   */
  protected class SAMVisitor extends MessageVisitor<Void> {
    public Void visit(StoreAsynchronousMessage message) {
      context_.acquireInboxHoldersReadRights();
      if (context_.getRecipients().contains(message.getRecipient())) {
        context_.acquireMessagesWriteRights();
        if (!context_.getWaitingAsynchronousMessagesMap().containsKey(message.getRecipient())) {
          context_.getWaitingAsynchronousMessagesMap().put(message.getRecipient(),
              new LinkedList<AsynchronousMessage>());
        }
        context_.getWaitingAsynchronousMessagesMap().get(message.getRecipient()).
            add(message.getMessage());
        context_.freeMessagesWriteRights();
      } else {
        logger_.warn("Received message to store for synchro-peer, but current instance is not" +
            " included in its synchro-group.");
      }
      context_.freeInboxHoldersReadRights();
      return null;
    }
  }

}
