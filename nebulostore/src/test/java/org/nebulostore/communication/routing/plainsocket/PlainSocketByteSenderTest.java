package org.nebulostore.communication.routing.plainsocket;

import java.util.concurrent.Executors;

import org.nebulostore.communication.routing.AbstractByteSenderTest;
import org.nebulostore.communication.routing.ByteSender;

/**
 * @author Grzegorz Milka
 */
public class PlainSocketByteSenderTest extends AbstractByteSenderTest {

  @Override
  protected ByteSender newByteSender() {
    return new PlainSocketByteSender(Executors.newSingleThreadExecutor());
  }

}
