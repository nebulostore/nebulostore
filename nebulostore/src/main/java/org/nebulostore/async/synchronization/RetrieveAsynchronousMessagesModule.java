package org.nebulostore.async.synchronization;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.messages.AsyncModuleErrorMessage;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.synchronization.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.synchronization.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.util.RecipientPeerData;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;
import org.nebulostore.utils.Pair;

/**
 * Class that exchanges asynchronous messages data with synchro-peers by creating
 * GetAsynchronousMessagesModule for each synchro-peer.
 *
 * @author szymonmatejczyk
 * @author Piotr Malicki
 */
public class RetrieveAsynchronousMessagesModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RetrieveAsynchronousMessagesModule.class);
  private static final long INSTANCE_TIMEOUT = 10000L;

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

  private final RAMVisitor visitor_ = new RAMVisitor();
  /**
   * States.
   *
   * @author szymonmatejczyk
   */
  private enum STATE {
    NONE, WAITING_FOR_MESSAGES
  }

  /**
   * Visitor.
   *
   * @author szymonmatejczyk
   * @author Piotr Malicki
   */
  protected class RAMVisitor extends MessageVisitor<Void> {
    private STATE state_ = STATE.NONE;
    private final Set<CommAddress> waitingForMessages_ = new HashSet<>();

    public Void visit(JobInitMessage message) {
      logger_.debug("Starting " + RetrieveAsynchronousMessagesModule.class + " for " +
          synchroGroupOwner_ + " with synchro-group: " + synchroGroup_);
      timer_.schedule(jobId_, INSTANCE_TIMEOUT);
      for (CommAddress inboxHolder : synchroGroup_) {
        RecipientPeerData peerData = context_.getRecipientPeerDataForPeer(synchroGroupOwner_);

        if (!inboxHolder.equals(myAddress_) &&
            (peerData.isRecipient() || synchroGroupOwner_.equals(myAddress_))) {
          GetAsynchronousMessagesMessage m =
              new GetAsynchronousMessagesMessage(message.getId(), myAddress_, inboxHolder,
                  synchroGroupOwner_, peerData.getClockValue());
          logger_.info("Sending get asynchronous messages request for " + synchroGroupOwner_ +
              " to peer " + inboxHolder + " as the peer to ask, request: " + m);
          networkQueue_.add(m);
          waitingForMessages_.add(inboxHolder);
        } else {
          logger_.info("Peer " + synchroGroupOwner_ + " is not present in the recipients set.");
        }
        state_ = STATE.WAITING_FOR_MESSAGES;
      }

      if (waitingForMessages_.isEmpty()) {
        finishModule();
      }
      return null;
    }

    public Void visit(AsynchronousMessagesMessage message) {
      logger_.debug("Retrieved next messages: " + message.getMessages() + " from " +
          message.getSourceAddress());
      if (!waitingForMessages_.remove(message.getSourceAddress()) ||
          !message.getRecipient().equals(synchroGroupOwner_) ||
          !state_.equals(STATE.WAITING_FOR_MESSAGES)) {
        logger_.warn("Received a message that was not expected.");
        return null;
      }

      if (message.getMessages() == null) {
        logger_.info("Empty AMM received from peer: " + message.getSourceAddress());
      } else {
        RecipientPeerData peerData = context_.getRecipientPeerDataForPeer(synchroGroupOwner_);
        if (peerData.isRecipient() || synchroGroupOwner_.equals(myAddress_)) {
          context_.updateRecipientDataForPeer(message.getRecipient(), message.getMessages(),
              message.getMessagesTimestamps(), message.getLastClearTimestamp(),
              message.getTimestamp());
          peerData.removeMessagesFromBeforeTimestamp(message.getTimestamp());
          AsynchronousMessagesMessage messages =
              new AsynchronousMessagesMessage(message.getId(), message.getDestinationAddress(),
                  message.getSourceAddress(), peerData.getMessages(), message.getRecipient(),
                  peerData.getClockValue(), peerData.getLastClearTimestamp(),
                  peerData.getMessagesTimestamps());
          networkQueue_.add(messages);
        } else {
          logger_.info("Peer " + synchroGroupOwner_ + " is not present in recipients set.");
        }
      }

      logger_.debug("WaitingForMessages size: " + waitingForMessages_.size());
      if (waitingForMessages_.isEmpty()) {
        handleReceivedMessages();
        finishModule();
      }
      return null;
    }

    public Void visit(AsyncModuleErrorMessage message) {
      if (state_.equals(STATE.WAITING_FOR_MESSAGES) &&
          waitingForMessages_.remove(message.getSourceAddress())) {
        logger_.warn("Received " + message.getClass() + " from " + message.getSourceAddress() +
            " in " + RetrieveAsynchronousMessagesModule.class);
        if (waitingForMessages_.isEmpty()) {
          handleReceivedMessages();
          finishModule();
        }
      } else {
        logger_.warn("Received unexpected message: " + message);
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      logger_.warn("Timeout in RetrieveAsynchronousMessagesModule.");
      logger_.debug(waitingForMessages_.size() + " get requests were not finished");
      handleReceivedMessages();
      endJobModule();
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      logger_.warn("Message " + message.getMessage() + " was not sent");
      if (state_.equals(STATE.WAITING_FOR_MESSAGES) &&
          message.getMessage().getSourceAddress().equals(myAddress_)) {
        waitingForMessages_.remove(message.getMessage().getDestinationAddress());
      }
      if (waitingForMessages_.isEmpty()) {
        handleReceivedMessages();
        finishModule();
      }
      return null;
    }

    private void handleReceivedMessages() {
      logger_.debug("Handling messages for peer: " + synchroGroupOwner_);
      if (synchroGroupOwner_.equals(myAddress_)) {
        logger_.debug("Before clearing");
        Set<Pair<AsynchronousMessage, VectorClockValue>> messages =
            context_.clearWaitingMessagesForPeer(myAddress_);
        logger_.debug("Handling received messages: " + messages);
        List<Pair<AsynchronousMessage, VectorClockValue>> messagesList =
            Lists.newLinkedList(messages);
        Collections.sort(messagesList,
            new Comparator<Pair<AsynchronousMessage, VectorClockValue>>() {
              @Override
              public int compare(Pair<AsynchronousMessage, VectorClockValue> firstMessage,
                  Pair<AsynchronousMessage, VectorClockValue> secondMessage) {
                return firstMessage.getSecond().compareTo(secondMessage.getSecond());
              }
            });
        for (Pair<AsynchronousMessage, VectorClockValue> msgWithTimestamp : messagesList) {
          logger_.debug("Handling next message: " + msgWithTimestamp);
          outQueue_.add(msgWithTimestamp.getFirst());
        }
      }
    }

    private void finishModule() {
      timer_.cancelTimer();
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
