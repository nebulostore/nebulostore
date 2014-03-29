package org.nebulostore.async;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.messages.DeleteNebuloObjectMessage;
import org.nebulostore.async.messages.UpdateNebuloObjectMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;


/**
 * Class used to handle GetAsynchronousMessagesInMessage.
 * Retrieves asynchronous messages from synchro peers by creating GetAsynchronousMessagesModule for
 * each synchro-peer.
 * @author szymonmatejczyk
 */
public class RetrieveAsynchronousMessagesModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RetrieveAsynchronousMessagesModule.class);
  private static final long INSTANCE_TIMEOUT = 10000L;
  public static final long EXECUTION_PERIOD = 5000L;

  private CommAddress myAddress_;
  private Timer timer_;
  private AsyncMessagesContext context_;

  @Inject
  public void setDependencies(CommAddress address, Timer timer,
      AsyncMessagesContext context) {
    timer_ = timer;
    myAddress_ = address;
    context_ = context;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
  private final RAMVisitor visitor_ = new RAMVisitor();

  /**
   * Visitor.
   * @author szymonmatejczyk
   */
  protected class RAMVisitor extends MessageVisitor<Void> {
    /** Start of download of AM. Requests for Metadata containing inboxHolders. */
    public Void visit(JobInitMessage message) {
      logger_.debug("Started asynchronous-messages retrieval.");
      jobId_ = message.getId();
      GetDHTMessage m = new GetDHTMessage(jobId_, myAddress_.toKeyDHT());
      networkQueue_.add(m);
      timer_.schedule(jobId_, INSTANCE_TIMEOUT);
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      error(jobId_, new NebuloException("Unable to get synchro peers from DHT."));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getKey().equals(myAddress_.toKeyDHT()) &&
          (message.getValue().getValue() instanceof InstanceMetadata)) {
        InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
        context_.setMyInboxHolders(metadata.getInboxHolders());
        logger_.debug("Retrieving AM from " + context_.getMyInboxHolders().size() + " peers.");
        //TODO(szm): timeouts
        for (CommAddress inboxHolder : context_.getMyInboxHolders()) {
          GetAsynchronousMessagesModule messagesModule =
              new GetAsynchronousMessagesModule(networkQueue_, inQueue_, inboxHolder);
          JobInitMessage initializingMessage = new JobInitMessage(messagesModule);
          context_.getWaitingForMessages().add(initializingMessage.getId());
        }
        if (context_.getMyInboxHolders().size() == 0) {
          endJobModule();
        }
      }
      return null;
    }

    public Void visit(AsynchronousMessagesMessage message) {
      if (!context_.getWaitingForMessages().remove(message.getId())) {
        logger_.warn("Received not expected message.");
      }

      if (message.getMessages() == null) {
        logger_.debug("Empty AMM received.");
      } else {
        for (AsynchronousMessage m : message.getMessages()) {
          // TODO(szm): Prevent message duplicates
          if (m instanceof UpdateNebuloObjectMessage) {
            // TODO(szm): update file
            logger_.debug("Received update file asynchronous message " +
                ((UpdateNebuloObjectMessage) m).getMessageId());
          } else if (m instanceof DeleteNebuloObjectMessage) {
            NebuloAddress address = ((DeleteNebuloObjectMessage) m).getObjectId();
            logger_.debug("Received delete asynchronous message " + address);
            // TODO(szm): delete file
          } else {
            error(message.getId(), new NebuloException("Unknown AsynchronousMessage type."));
          }
        }
      }
      if (context_.getWaitingForMessages().isEmpty()) {
        endJobModule();
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.debug("Timeout in RetrieveAsynchronousMessagesModule.");
      endJobModule();
      return null;
    }

    private void error(String jobId, NebuloException error) {
      logger_.warn("Unable to retrive asynchronous messages: " + error.getMessage());
      endJobModule();
    }
  }
}
