package org.nebulostore.persistence;


import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

public class SQLKeyValueStoreTest extends KeyValueStoreTestTemplate {

  @Override
  protected KeyValueStore<String> getKeyValueStore() throws IOException {
    return new SQLKeyValueStore<String>(
        new Function<String, byte[]>() {
          @Override
          public byte[] apply(String str) {
            return str.getBytes(Charsets.UTF_8);
          }
        }, new Function<byte[], String>() {
          @Override
          public String apply(byte[] bytes) {
            return new String(bytes, Charsets.UTF_8);
          }
        });
  }

}
