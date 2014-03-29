package org.nebulostore.async;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessagesMessage;
import org.nebulostore.async.messages.GetAsynchronousMessagesMessage;
import org.nebulostore.async.messages.GotAsynchronousMessagesMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Module that downloads asynchronous messages from a synchro peer and sends them to
 * resultQueue_.
 * @author szymonmatejczyk
 */
public class GetAsynchronousMessagesModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(GetAsynchronousMessagesModule.class);

  /** Parent module. Used to return downloaded messages.
   */
  private final BlockingQueue<Message> resultQueue_;

  /**
   * Peer, from that this module downloads messages.
   */
  private final CommAddress synchroPeer_;
  private CommAddress myAddress_;

  public GetAsynchronousMessagesModule(BlockingQueue<Message> networkQueue,
      BlockingQueue<Message> resultQueue, CommAddress synchroPeer) {
    setNetworkQueue(networkQueue);
    resultQueue_ = resultQueue;
    synchroPeer_ = synchroPeer;
  }

  @Inject
  public void setCommAddress(CommAddress commAddress) {
    myAddress_ = commAddress;
  }

  private final GetAsynchronousMessagesVisitor visitor_ = new GetAsynchronousMessagesVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * States.
   * @author szymonmatejczyk
   */
  private enum STATE { NONE, WAITING_FOR_MESSAGES }

  /**
   * Visitor handling this module messages.
   * @author szymonmatejczyk
   */
  protected class GetAsynchronousMessagesVisitor extends MessageVisitor<Void> {
    private STATE state_ = STATE.NONE;

    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      GetAsynchronousMessagesMessage m = new GetAsynchronousMessagesMessage(message.getId(),
          null, synchroPeer_, myAddress_);
      networkQueue_.add(m);
      state_ = STATE.WAITING_FOR_MESSAGES;
      return null;
    }

    public Void visit(AsynchronousMessagesMessage message) {
      if (state_ != STATE.WAITING_FOR_MESSAGES) {
        logger_.warn("AsynchronousMessages(" + message.getId() + ") unexpected in this state.");
        return null;
      }

      GotAsynchronousMessagesMessage ackMessage =
          new GotAsynchronousMessagesMessage(message.getId(), message.getDestinationAddress(),
              message.getSourceAddress());
      networkQueue_.add(ackMessage);
      AsynchronousMessagesMessage m = new AsynchronousMessagesMessage(getJobId(), null, null,
          message.getMessages());
      resultQueue_.add(m);
      endJobModule();
      return null;
    }
  }
}
