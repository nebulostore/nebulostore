package org.nebulostore.replicaresolver;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;

import org.nebulostore.communication.netutils.remotemap.RemoteMapFactory;


/**
 * @author Grzegorz Milka
 */
public class ReplicaResolverFactoryImpl implements ReplicaResolverFactory {
  private final RemoteMapFactory remoteMapFactory_;
  private final AtomicBoolean hasStarted_ = new AtomicBoolean(false);
  private final AtomicBoolean hasStopped_ = new AtomicBoolean(false);
  private ReplicaResolver contractMap_;

  @Inject
  public ReplicaResolverFactoryImpl(RemoteMapFactory remoteMapFactory) {
    remoteMapFactory_ = remoteMapFactory;
  }

  @Override
  public ReplicaResolver getContractMap() {
    if (!hasStarted_.get() || hasStopped_.get()) {
      throw new IllegalStateException();
    }
    return contractMap_;
  }

  @Override
  public void startUp() throws IOException {
    contractMap_ = new ReplicaResolverAdapter(remoteMapFactory_.getRemoteMap());
    hasStarted_.set(true);
  }

  @Override
  public void shutDown() {
    hasStopped_.set(true);
  }
}
