package org.nebulostore.communication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

import org.nebulostore.appcore.modules.Module;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.peers.GenericConfiguration;
import org.nebulostore.replicaresolver.BDBPeerToReplicaResolverAdapter;
import org.nebulostore.replicaresolver.DHTPeerFactory;
import org.nebulostore.replicaresolver.ReplicaResolverFactory;
import org.nebulostore.replicaresolver.ReplicaResolverFactoryImpl;

/**
 * @author Grzegorz Milka
 */
public class CommunicationFacadeAdapterConfiguration extends GenericConfiguration {
  private static final int DHT_EXECUTOR_THREAD_POOL_SIZE = 4;

  @Override
  protected final void configureAll() {
    configureLocalCommAddress();

    AbstractModule commModule = createCommunicationFacadeConfiguration();

    install(commModule);

    bind(ReplicaResolverFactory.class).to(ReplicaResolverFactoryImpl.class).in(Singleton.class);

    install(new FactoryModuleBuilder().implement(Runnable.class,
        CommunicationFacadeAdapter.class).build(CommunicationPeerFactory.class));

    configureDHT();
  }

  protected void configureLocalCommAddress() {
    bind(CommAddress.class).annotatedWith(Names.named("LocalCommAddress")).
    toInstance(new CommAddress(
        config_.getString("communication.comm-address", "")));
    bind(CommAddress.class).annotatedWith(Names.named("communication.local-comm-address")).
        toInstance(new CommAddress(
            config_.getString("communication.comm-address", "")));
  }

  @Provides
  @Named("communication.main-executor")
  @Singleton
  ExecutorService provideMainExecutor() {
    return Executors.newCachedThreadPool();
  }

  protected CommunicationFacadeConfiguration createCommunicationFacadeConfiguration() {
    return new CommunicationFacadeConfiguration(config_);
  }

  private void configureDHT() {
    install(new FactoryModuleBuilder().implement(Module.class,
        BDBPeerToReplicaResolverAdapter.class).build(DHTPeerFactory.class));
    bind(ExecutorService.class).annotatedWith(Names.named("communication.dht.executor")).toInstance(
        Executors.newFixedThreadPool(DHT_EXECUTOR_THREAD_POOL_SIZE));
  }

}
