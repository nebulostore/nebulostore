package org.nebulostore.async.checker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.Module;
import org.nebulostore.async.checker.messages.TickMessage;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.InDHTMessage;
import org.nebulostore.dht.messages.OutDHTMessage;
import org.nebulostore.timer.Timer;

/**
 * Module that checks if messages requiring acknowlegments of receiving were delivered. If it
 * doesn't receive acknowledgment for a message after ACK_TIMEOUT_MILIS miliseconds, error responder
 * for this message is run.
 *
 * @author Piotr Malicki
 *
 */
public class MessageReceivingCheckerModule extends Module {
  private static Logger logger_ = Logger.getLogger(MessageReceivingCheckerModule.class);

  public static final long TICK_PERIOD_MILIS = 1000;
  public static final long ACK_TIMEOUT_MILIS = 4000;

  private final MessageVisitor visitor_ = new MRCVisitor();

  private final SortedSet<MessageWithTimestamp> messages_ = new TreeSet<>();
  private final Map<String, MessageWithTimestamp> messagesMap_ = new HashMap<>();
  private final Timer timer_;
  private final CommAddress myAddress_;
  private final BlockingQueue<Message> networkQueue_;
  private final BlockingQueue<Message> dispatcherQueue_;

  @Inject
  public MessageReceivingCheckerModule(Timer timer, CommAddress myAddress,
      @Named("MsgReceivingCheckerNetworkQueue") BlockingQueue<Message> networkQueue,
      @Named("MsgReceivingCheckerOutQueue") BlockingQueue<Message> outQueue,
      @Named("MsgReceivingCheckerInQueue") BlockingQueue<Message> inQueue,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    super(inQueue, outQueue);
    timer_ = timer;
    myAddress_ = myAddress;
    networkQueue_ = networkQueue;
    dispatcherQueue_ = dispatcherQueue;

  }

  protected class MRCVisitor extends MessageVisitor {

    public void visit(CommMessage message) {
      logger_.debug("Received comm message: " + message);
      if (message.getSourceAddress() == null) {
        message.setSourceAddress(myAddress_);
      }
      if (message.getDestinationAddress() == null) {
        logger_.warn("Received message with null destination address: " + message);
      } else if (message.getDestinationAddress().equals(myAddress_)) {
        if (message.requiresAck()) {
          logger_.debug("Sending ack for message: " + message);
          networkQueue_.add(new MessageReceivedMessage(message.getDestinationAddress(), message
              .getSourceAddress(), message.getMessageId()));
        }
        outQueue_.add(message);
      } else {
        networkQueue_.add(message);
        if (message.requiresAck()) {
          MessageWithTimestamp msg =
              new MessageWithTimestamp(message, System.currentTimeMillis() + ACK_TIMEOUT_MILIS);
          messages_.add(msg);
          messagesMap_.put(message.getMessageId(), msg);
        }
      }
    }

    public void visit(TickMessage message) {
      logger_.debug("Tick message received, current messages set: " + messages_);
      long currentTime = System.currentTimeMillis();
      for (Iterator<MessageWithTimestamp> iterator = messages_.iterator(); iterator.hasNext();) {
        MessageWithTimestamp nextMessage = iterator.next();
        if (currentTime >= nextMessage.timestamp_) {
          // message wasn't received, sending it asynchronously
          logger_.debug("Timeout, sending the message " + nextMessage + " asynchronously.");
          nextMessage.message_.generateErrorResponder(dispatcherQueue_).handleError();
          iterator.remove();
          messagesMap_.remove(nextMessage.message_.getMessageId());
        } else {
          break;
        }
      }
    }

    public void visit(MessageReceivedMessage message) {
      MessageWithTimestamp msg = messagesMap_.remove(message.getOriginalMessageId());
      if (msg == null) {
        logger_.warn("Received ack message for message not in module's messages set.");
      } else {
        logger_.debug("Received ack for message: " + message);
        messages_.remove(msg);
      }
    }

    public void visit(InDHTMessage message) {
      networkQueue_.add(message);
    }

    public void visit(OutDHTMessage message) {
      outQueue_.add(message);
    }

    public void visit(ErrorCommMessage message) {
      outQueue_.add(message);
    }

    public void visit(EndModuleMessage message) {
      //Try to send all remaining messages asynchronously before ending the module
      timer_.cancelTimer();
      for (MessageWithTimestamp msg : messages_) {
        logger_.debug("Ending the module, sending remaining messages: " + message);
        msg.message_.generateErrorResponder(dispatcherQueue_).handleError();
      }
      logger_.debug("Forwarding EndModuleMessage to the next module");
      networkQueue_.add(message);
      logger_.debug("Ending the module");
      endModule();
    }
  }

  @Override
  protected void initModule() {
    super.initModule();
    timer_.scheduleRepeated(new TickMessage(), TICK_PERIOD_MILIS, TICK_PERIOD_MILIS);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  private class MessageWithTimestamp implements Comparable<MessageWithTimestamp> {

    private final CommMessage message_;
    private final long timestamp_;

    public MessageWithTimestamp(CommMessage message, long timestamp) {
      message_ = message;
      timestamp_ = timestamp;
    }

    @Override
    public int compareTo(MessageWithTimestamp msg) {
      if (timestamp_ < msg.timestamp_) {
        return -1;
      } else if (timestamp_ > msg.timestamp_) {
        return 1;
      } else {
        return message_.compareTo(msg.message_);
      }
    }

    @Override
    public String toString() {
      return "[Message: " + message_ + ", Timestamp: " + timestamp_ + "]";
    }
  }

}
