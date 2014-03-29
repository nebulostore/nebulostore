package org.nebulostore.subscription.model;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.nebulostore.communication.naming.CommAddress;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class holds subscribers of a NebuloFile.
 * Author: rafalhryciuk.
 */
public class Subscribers implements Serializable {
  private static final long serialVersionUID = 1155986060086428652L;
  private final Set<CommAddress> subscribers_;


  public Subscribers() {
    subscribers_ = new HashSet<CommAddress>();
  }

  /**
   * Adds new subscriber.
   * @param subscriber Address of a new subscriber of the file
   * @return false if given subscriber is already registered.
   */
  public boolean addSubscriber(CommAddress subscriber) {
    checkNotNull(subscriber);
    return subscribers_.add(subscriber);
  }

  /**
   *
   * @param subscriber Address of a subscriber to delete.
   * @return false if there is no subscriber registered with given CommAddress.
   */
  public boolean removesSubscriber(CommAddress subscriber) {
    checkNotNull(subscriber);
    return subscribers_.remove(subscriber);
  }

  public Set<CommAddress> getSubscribersAddresses() {
    return ImmutableSet.copyOf(subscribers_);
  }
}
