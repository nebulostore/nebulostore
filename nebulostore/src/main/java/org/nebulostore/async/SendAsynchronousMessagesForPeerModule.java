package org.nebulostore.async;

import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.StoreAsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Sends asynchronous message to all peers' sycnhro-peers.
 *
 * We give no guarantee on asynchronous messages.
 * @author szymonmatejczyk
 *
 */
public class SendAsynchronousMessagesForPeerModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(ResponseWithAsynchronousMessagesModule.class);

  private final CommAddress recipient_;
  private final AsynchronousMessage message_;

  public SendAsynchronousMessagesForPeerModule(CommAddress recipient,
      AsynchronousMessage message, BlockingQueue<Message> dispatcherQueue) {
    recipient_ = recipient;
    message_ = message;
    outQueue_ = dispatcherQueue;
    runThroughDispatcher();
  }

  private final MessageVisitor<Void> visitor_ = new SendAsynchronousMessagesForPeerModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Visitor.
   */
  public class SendAsynchronousMessagesForPeerModuleVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      GetDHTMessage m = new GetDHTMessage(jobId_, recipient_.toKeyDHT());
      networkQueue_.add(m);
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(recipient_.toKeyDHT()) &&
          (message.getValue().getValue() instanceof InstanceMetadata)) {
        InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
        for (CommAddress inboxHolder : metadata.getInboxHolders()) {
          networkQueue_.add(new StoreAsynchronousMessage(jobId_, null, inboxHolder,
              recipient_, message_));
        }
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      logger_.error("Sending asynchronous messages for " + recipient_.toString() + " failed...");
      return null;
    }
  }

}
