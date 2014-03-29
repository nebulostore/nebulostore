package org.nebulostore.appcore.context;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Guice context with default values.
 * @author Bolek Kulbabinski
 */
public class DefaultTestContext extends AbstractModule {
  @Override
  protected void configure() {
    bind(AppKey.class).toInstance(new AppKey("1"));
    bind(CommAddress.class).toInstance(new CommAddress("2"));
    bind(new TypeLiteral<BlockingQueue<Message>>() { })
      .annotatedWith(Names.named("NetworkQueue"))
      .toInstance(new LinkedBlockingQueue<Message>());
    bind(new TypeLiteral<BlockingQueue<Message>>() { })
      .annotatedWith(Names.named("DispatcherQueue"))
      .toInstance(new LinkedBlockingQueue<Message>());
  }
}
