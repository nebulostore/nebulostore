package org.nebulostore.communication.routing;

import org.junit.Before;

/**
 *
 * @author Grzegorz Milka
 */
public class CachedOOSDispatcherStressTest extends AbstractOOSDispatcherStressTest {
  @Before
  @Override
  public void setUp() {
    /* Short time interval to increase coverage */
    dispatcher_ = new CachedOOSDispatcher(80);
    super.setUp();
  }


}
