package org.nebulostore.communication.netutils.remotemap;

import java.io.IOException;
import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.nebulostore.persistence.KeyValueStore;

public class RemoteMapImpl implements RemoteMap {
  private static final String SEPARATOR = "___";

  private final KeyValueStore<Serializable> store_;

  public RemoteMapImpl(KeyValueStore<Serializable> store) {
    store_ = store;
  }

  @Override
  public Serializable get(int type, String key) throws IOException {
    return store_.get(getKey(type, key));
  }

  @Override
  public void put(int type, String key, Serializable value)
      throws IOException {
    store_.put(getKey(type, key), value);
  }

  @Override
  public void performTransaction(final int type, final String sKey, final Transaction transaction)
      throws IOException {
    final String key = getKey(type, sKey);
    store_.performTransaction(key, new Function<Serializable, Serializable>() {

      @Override
      public Serializable apply(Serializable arg) {
        return transaction.performTransaction(type, key, arg);
      }

    });
  }

  private String getKey(int type, String key) {
    Preconditions.checkArgument(!key.contains(SEPARATOR), "Key cannot contain sequence '" +
        SEPARATOR + "'");
    return type + SEPARATOR + key;
  }

}
