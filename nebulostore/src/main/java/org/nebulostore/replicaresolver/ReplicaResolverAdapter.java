package org.nebulostore.replicaresolver;

import java.io.IOException;

import org.nebulostore.communication.netutils.remotemap.RemoteMap;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;

/**
 * @author Grzegorz Milka
 */
public class ReplicaResolverAdapter implements ReplicaResolver {
  private static final int CONTRACT_TYPE = 1;
  private final RemoteMap remoteMap_;

  public ReplicaResolverAdapter(RemoteMap remoteMap) {
    remoteMap_ = remoteMap;
  }

  /**
    * Gives current ValueDHT for given key.
    *
    * @return null if no mapping exists.
    * @throws IOException
    */
  @Override
  public ValueDHT get(KeyDHT key) throws IOException {
    return (ValueDHT) remoteMap_.get(CONTRACT_TYPE, key.toString());
  }

  @Override
  public void put(KeyDHT key, ValueDHT value) throws IOException {
    remoteMap_.performTransaction(CONTRACT_TYPE, key.toString(), new MergeTransaction(value));
  }
}
