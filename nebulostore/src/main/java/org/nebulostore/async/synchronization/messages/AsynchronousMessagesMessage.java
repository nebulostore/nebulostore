package org.nebulostore.async.synchronization.messages;

import java.util.Map;
import java.util.Set;

import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.synchronization.VectorClockValue;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Collection of messages sent for recipient_.
 * @author szymonmatejczyk
 */
public class AsynchronousMessagesMessage extends CommMessage {
  private static final long serialVersionUID = -2023624456880608658L;

  private final Set<AsynchronousMessage> messages_;
  private final CommAddress recipient_;
  /**
   * Sender's vector clock value for recipient_'s synchro group at the
   * moment of creating the message.
   */
  private final VectorClockValue timestamp_;

  /**
   * Vector clock at the moment of last clear of recipient_'s messages set as remembered
   * by the sender.
   */
  private final VectorClockValue lastClearTimestamp_;

  /**
   * Map of timestamps for each message id from messages_ set.
   */
  private final Map<String, VectorClockValue> messagesTimestamps_;

  public AsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress);
    messages_ = null;
    recipient_ = null;
    timestamp_ = null;
    lastClearTimestamp_ = null;
    messagesTimestamps_ = null;
  }

  public AsynchronousMessagesMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, Set<AsynchronousMessage> messages, CommAddress recipient,
      VectorClockValue timestamp, VectorClockValue lastClearTimestamp,
      Map<String, VectorClockValue> messagesTimestamps) {
    super(jobId, sourceAddress, destAddress);
    messages_ = messages;
    recipient_ = recipient;
    timestamp_ = timestamp;
    lastClearTimestamp_ = lastClearTimestamp;
    messagesTimestamps_ = messagesTimestamps;
  }

  public Set<AsynchronousMessage> getMessages() {
    return messages_;
  }

  public CommAddress getRecipient() {
    return recipient_;
  }

  public VectorClockValue getTimestamp() {
    return timestamp_;
  }

  public VectorClockValue getLastClearTimestamp() {
    return lastClearTimestamp_;
  }

  public Map<String, VectorClockValue> getMessagesTimestamps() {
    return messagesTimestamps_;
  }
}
