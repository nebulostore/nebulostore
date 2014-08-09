package org.nebulostore.systest.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.peerselection.AlwaysDenyingSynchroPeerSelectionModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModuleFactory;
import org.nebulostore.peers.AbstractPeer;
import org.nebulostore.systest.TestingPeerConfiguration;

public class AsyncTestingPeerConfiguration extends TestingPeerConfiguration {

  private static Logger logger_ = Logger.getLogger(AsyncTestingPeerConfiguration.class);

  @Override
  protected void configurePeer() {
    bind(AbstractPeer.class).to(AsyncTestingPeer.class);
  }

  @Override
  protected void configureAsyncMessaging() {
    bind(AsyncMessagesContext.class).toInstance(new AsyncMessagesContext());
    bind(ScheduledExecutorService.class).annotatedWith(
        Names.named("async.scheduled-executor-service")).toInstance(
        Executors.newScheduledThreadPool(1));
    install(new FactoryModuleBuilder().implement(SynchroPeerSelectionModule.class,
        AlwaysDenyingSynchroPeerSelectionModule.class).build(
        SynchroPeerSelectionModuleFactory.class));
  }

  @Override
  protected void configureAdditional() {
    super.configureAdditional();
    bind(CounterModule.class).toInstance(new CounterModule());
    logger_.debug("Additional modules configured.");
  }

  @Override
  protected void configureQueues() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> dispatcherQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> commPeerInQueue = new LinkedBlockingQueue<Message>();

    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("NetworkQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommunicationPeerInQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("DispatcherQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommunicationPeerOutQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommOverlayInQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommOverlayNetworkQueue")).toInstance(commPeerInQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommOverlayOutQueue")).toInstance(dispatcherQueue);
    logger_.debug("Communication queues configured.");
  }
}
