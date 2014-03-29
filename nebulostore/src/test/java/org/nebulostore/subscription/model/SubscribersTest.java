package org.nebulostore.subscription.model;

import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;



/**
 * Author: rafalhryciuk.
 */
public class SubscribersTest {

  private static final CommAddress ADDRESS = new CommAddress(1, 1);

  private Subscribers subscribers_;


  @Before
  public void setUp() {
    subscribers_ = new Subscribers();
    subscribers_.addSubscriber(ADDRESS);
  }

  @Test
  public void shouldAddSubscriber() {
    CommAddress address = new CommAddress(2, 2);
    final int subscribersNumber = subscribers_.getSubscribersAddresses().size();

    boolean expandSubscribers = subscribers_.addSubscriber(address);

    assertTrue(expandSubscribers);
    assertEquals(subscribersNumber + 1, subscribers_.getSubscribersAddresses().size());
  }

  @Test
  public void shouldNotAddDuplicateOfSubscriber() {
    boolean expandSubscribers = subscribers_.addSubscriber(ADDRESS);

    assertFalse(expandSubscribers);
    assertEquals(1, subscribers_.getSubscribersAddresses().size());
  }

  @Test
  public void shouldReturnEmptySetIfNoSubscribersAreAdded() {
    Subscribers localSubscribers = new Subscribers();

    Set<CommAddress> subscribersAddresses =  localSubscribers.getSubscribersAddresses();
    assertTrue(subscribersAddresses.isEmpty());
  }

  @Test
  public void shouldRemoveSubscriber() {
    boolean subscriberRemoved = subscribers_.removesSubscriber(ADDRESS);

    assertTrue(subscriberRemoved);
    assertTrue(subscribers_.getSubscribersAddresses().isEmpty());
  }

  @Test
  public void shouldNotRemoveFromEmptySubscribers() {
    Subscribers localSubscribers = new Subscribers();

    boolean removedSubscriber = localSubscribers.removesSubscriber(ADDRESS);

    assertFalse(removedSubscriber);
  }
}
