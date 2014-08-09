package org.nebulostore.async;

import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Class that retrieves asynchronous messages from synchro-peers by creating
 * GetAsynchronousMessagesModule for each synchro-peer.
 *
 * @author szymonmatejczyk
 */
public class RetrieveAsynchronousMessagesModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RetrieveAsynchronousMessagesModule.class);
  private static final long INSTANCE_TIMEOUT = 10000L;
  public static final long EXECUTION_PERIOD = 5000L;

  private CommAddress myAddress_;
  private Timer timer_;
  private AsyncMessagesContext context_;

  private final Set<CommAddress> synchroGroup_;
  private final CommAddress synchroGroupOwner_;

  public RetrieveAsynchronousMessagesModule(Set<CommAddress> synchroGroup,
      CommAddress synchroGroupOwner) {
    synchroGroup_ = synchroGroup;
    synchroGroupOwner_ = synchroGroupOwner;
  }

  @Inject
  public void setDependencies(CommAddress address, Timer timer, AsyncMessagesContext context) {
    timer_ = timer;
    myAddress_ = address;
    context_ = context;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private final RADVisitor visitor_ = new RADVisitor();

  /**
   * Visitor.
   *
   * @author szymonmatejczyk
   */
  protected class RADVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      timer_.schedule(jobId_, INSTANCE_TIMEOUT);
      for (CommAddress inboxHolder : synchroGroup_) {
        if (!inboxHolder.equals(myAddress_)) {
          GetAsynchronousMessagesModule messagesModule = new GetAsynchronousMessagesModule(
              networkQueue_, inQueue_, inboxHolder, synchroGroupOwner_);
          JobInitMessage initializingMessage = new JobInitMessage(messagesModule);
          context_.getWaitingForMessages(jobId_).add(initializingMessage.getId());
          outQueue_.add(initializingMessage);
        }
      }
      if (synchroGroup_.size() == 0) {
        endJobModule();
      }
      return null;
    }

    public Void visit(AsynchronousMessagesMessage message) {
      if (!context_.getWaitingForMessages(jobId_).remove(message.getId())) {
        logger_.warn("Received a message that was not expected.");
      }


      if (message.getMessages() == null) {
        logger_.debug("Empty AMM received.");
      } else if (message.getRecipient().equals(myAddress_)) {
        for (Message msg : message.getMessages()) {
          outQueue_.add(msg);
        }
      } else {
        //TODO (pm) Store new asynchronous messages for other peers
        hashCode();
      }

      if (context_.getWaitingForMessages(jobId_).isEmpty()) {
        endJobModule();
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout in RetrieveAsynchronousDataModule.");
      endJobModule();
      return null;
    }
  }
}
