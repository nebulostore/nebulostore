package org.nebulostore.peers;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.base.Functions;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import org.apache.commons.configuration.XMLConfiguration;
import org.nebulostore.api.DeleteNebuloObjectModule;
import org.nebulostore.api.GetNebuloObjectModule;
import org.nebulostore.api.WriteNebuloObjectModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.appcore.model.NebuloObjectFactoryImpl;
import org.nebulostore.appcore.model.ObjectDeleter;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.appcore.model.ObjectWriter;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.peerselection.AlwaysAcceptingSynchroPeerSelectionModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModuleFactory;
import org.nebulostore.broker.Broker;
import org.nebulostore.broker.BrokerContext;
import org.nebulostore.broker.ContractsEvaluator;
import org.nebulostore.broker.ContractsSelectionAlgorithm;
import org.nebulostore.broker.GreedyContractsSelection;
import org.nebulostore.broker.OnlySizeContractsEvaluator;
import org.nebulostore.broker.ValuationBasedBroker;
import org.nebulostore.communication.CommunicationFacadeAdapterConfiguration;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.DefaultConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.networkmonitor.NetworkMonitorImpl;
import org.nebulostore.persistence.FileStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.ReplicatorImpl;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.rest.BrokerResource;
import org.nebulostore.rest.NetworkMonitorResource;
import org.nebulostore.rest.RestModule;
import org.nebulostore.rest.RestModuleImpl;
import org.nebulostore.subscription.api.SimpleSubscriptionNotificationHandler;
import org.nebulostore.subscription.api.SubscriptionNotificationHandler;
import org.nebulostore.timer.Timer;
import org.nebulostore.timer.TimerImpl;

/**
 * Configuration (all dependencies and constants) of a regular Nebulostore peer.
 *
 * @author Bolek Kulbabinski
 */
public class PeerConfiguration extends GenericConfiguration {

  @Override
  protected void configureAll() {
    bind(XMLConfiguration.class).toInstance(config_);

    AppKey appKey = new AppKey(config_.getString("app-key", ""));
    bind(AppKey.class).toInstance(appKey);
    bind(CommAddress.class).toInstance(
        new CommAddress(config_.getString("communication.comm-address", "")));

    configureQueues();

    bind(NebuloObjectFactory.class).to(NebuloObjectFactoryImpl.class);
    bind(ObjectGetter.class).to(GetNebuloObjectModule.class);
    bind(ObjectWriter.class).to(WriteNebuloObjectModule.class);
    bind(ObjectDeleter.class).to(DeleteNebuloObjectModule.class);

    bind(SubscriptionNotificationHandler.class).to(SimpleSubscriptionNotificationHandler.class);

    bind(Timer.class).to(TimerImpl.class);


    configureAdditional();
    configureBroker();
    configureCommunicationPeer();
    configureNetworkMonitor();
    configureAsyncMessaging();
    configurePeer();
    configureReplicator(appKey);
    configureRestModule();
  }

  private void configureReplicator(AppKey appKey) {
    KeyValueStore<byte[]> replicatorStore;
    try {
      String pathPrefix = config_.getString("replicator.storage-path") + "/" +
        appKey.getKey().toString() + "_storage/";
      replicatorStore = new FileStore<byte[]>(pathPrefix,
        Functions.<byte[]>identity(), Functions.<byte[]>identity());
    } catch (IOException e) {
      throw new RuntimeException("Unable to configure Replicator module", e);
    }
    bind(new TypeLiteral<KeyValueStore<byte[]>>() { }).
      annotatedWith(Names.named("ReplicatorStore")).toInstance(replicatorStore);
    bind(Replicator.class).to(ReplicatorImpl.class);
  }

  protected void configureAdditional() {
  }

  protected void configurePeer() {
    bind(AbstractPeer.class).to(Peer.class);
  }

  protected void configureCommunicationPeer() {
    GenericConfiguration genConf;
    genConf = new CommunicationFacadeAdapterConfiguration();
    genConf.setXMLConfig(config_);
    install(genConf);
  }

  protected void configureBroker() {
    bind(Broker.class).to(ValuationBasedBroker.class).in(Scopes.SINGLETON);
    bind(ContractsSelectionAlgorithm.class).to(GreedyContractsSelection.class);
    bind(ContractsEvaluator.class).to(OnlySizeContractsEvaluator.class);
    bind(BrokerContext.class).toInstance(new BrokerContext());
  }

  protected void configureNetworkMonitor() {
    bind(NetworkMonitor.class).to(NetworkMonitorImpl.class).in(Scopes.SINGLETON);
    bind(ConnectionTestMessageHandler.class).to(DefaultConnectionTestMessageHandler.class);
  }

  protected void configureAsyncMessaging() {
    bind(AsyncMessagesContext.class).toInstance(new AsyncMessagesContext());
    bind(ScheduledExecutorService.class).annotatedWith(
        Names.named("async.scheduled-executor-service")).toInstance(
        Executors.newScheduledThreadPool(1));
    install(new FactoryModuleBuilder().implement(SynchroPeerSelectionModule.class,
        AlwaysAcceptingSynchroPeerSelectionModule.class).build(
        SynchroPeerSelectionModuleFactory.class));
  }

  protected void configureQueues() {
    BlockingQueue<Message> networkQueue = new LinkedBlockingQueue<Message>();
    BlockingQueue<Message> dispatcherQueue = new LinkedBlockingQueue<Message>();

    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("NetworkQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommunicationPeerInQueue")).toInstance(networkQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("DispatcherQueue")).toInstance(dispatcherQueue);
    bind(new TypeLiteral<BlockingQueue<Message>>() {
    }).annotatedWith(Names.named("CommunicationPeerOutQueue")).toInstance(dispatcherQueue);
  }

  protected void configureRestModule() {
    bind(BrokerResource.class);
    bind(NetworkMonitorResource.class);
    bind(RestModule.class).to(RestModuleImpl.class);
  }
}
