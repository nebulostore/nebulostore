package org.nebulostore.async.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.async.synchronization.VectorClockValue;

/**
 * Class containing all information from the context connected with concrete recipient peer.
 *
 * @author Piotr Malicki
 *
 */
public class RecipientPeerData {

  private final Set<AsynchronousMessage> messages_;
  private final VectorClockValue lastClearTimestamp_;
  private final Map<String, VectorClockValue> messagesTimestamps_;
  private final VectorClockValue clockValue_;
  private final boolean isRecipient_;

  public RecipientPeerData(Set<AsynchronousMessage> messages, VectorClockValue lastClearTimestamp,
      Map<String, VectorClockValue> messagesTimestamps, VectorClockValue clockValue,
      boolean isRecipient) {
    messages_ = messages;
    lastClearTimestamp_ = lastClearTimestamp;
    messagesTimestamps_ = messagesTimestamps;
    clockValue_ = clockValue;
    isRecipient_ = isRecipient;
  }

  public Set<AsynchronousMessage> getMessages() {
    return messages_;
  }

  public VectorClockValue getLastClearTimestamp() {
    return lastClearTimestamp_;
  }

  public Map<String, VectorClockValue> getMessagesTimestamps() {
    return messagesTimestamps_;
  }

  public VectorClockValue getClockValue() {
    return clockValue_;
  }

  public boolean isRecipient() {
    return isRecipient_;
  }

  public void removeMessagesFromBeforeTimestamp(VectorClockValue timestamp) {
    Iterator<AsynchronousMessage> iterator = messages_.iterator();
    while (iterator.hasNext()) {
      AsynchronousMessage msg = iterator.next();
      if (messagesTimestamps_.get(msg.getMessageId())
          .areAllElementsLowerEqual(timestamp)) {
        messagesTimestamps_.remove(msg.getMessageId());
        iterator.remove();
      }
    }
  }
}
