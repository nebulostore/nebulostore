package org.nebulostore.communication.routing;

import org.junit.Before;

/**
 * @author Grzegorz Milka
 */
public final class SimpleTCPConnectionInitiatorTest extends AbstractConnectionInitatorTest {
  @Before
  public void setUp() {
    initiator_ = new SimpleTCPConnectionInitator();
  }
}
