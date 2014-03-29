package org.nebulostore.async;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Persistent data shared by parts of asynchronous messages module.
 *
 * @author szymon
 *
 */
public class AsyncMessagesContext {

  /**
   * Messages waiting to be retrieved by peers.
   */
  private Map<CommAddress, List<AsynchronousMessage>> waitingAsynchronousMessagesMap_ =
      new HashMap<CommAddress, List<AsynchronousMessage>>();

  /**
   * CommAddresses of peers, that retrieved AM, but haven't sent response yet.
   */
  private Set<CommAddress> waitingForAck_ = new HashSet<CommAddress>();

  /**
   * Synchro-peers of this instance. Cached from DHT.
   */
  private List<CommAddress> myInboxHolders_ = new LinkedList<CommAddress>();

  /**
   * JobId of jobs we are waiting for response from.
   */
  private Set<String> waitingForMessages_ = new HashSet<String>();

  public List<CommAddress> getMyInboxHolders() {
    return myInboxHolders_;
  }

  public Map<CommAddress, List<AsynchronousMessage>> getWaitingAsynchronousMessages() {
    return waitingAsynchronousMessagesMap_;
  }

  public Set<CommAddress> getWaitingForAck() {
    return waitingForAck_;
  }

  public Set<String> getWaitingForMessages() {
    return waitingForMessages_;
  }

  public void setMyInboxHolders(List<CommAddress> myInboxHolders) {
    myInboxHolders_ = myInboxHolders;
  }

}
