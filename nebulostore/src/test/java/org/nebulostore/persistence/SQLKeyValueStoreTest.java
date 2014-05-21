package org.nebulostore.persistence;


import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class SQLKeyValueStoreTest extends KeyValueStoreTestTemplate {

  private static final String CONFIGURATION_PATH = System.getProperty("user.dir") +
      "/../resources/conf/Peer.xml.template";

  @Override
  protected KeyValueStore<String> getKeyValueStore() throws IOException {
    XMLConfiguration config = null;
    try {
      config = new XMLConfiguration(CONFIGURATION_PATH);
    } catch (ConfigurationException e) {
      throw new IOException(e.getMessage());
    }

    String host = config.getString("persistance.sql-keyvalue-store.host");
    String port = config.getString("persistance.sql-keyvalue-store.port");
    String database = config.getString("persistance.sql-keyvalue-store.database");
    String user = config.getString("persistance.sql-keyvalue-store.user");
    String password = config.getString("persistance.sql-keyvalue-store.password");
    boolean canUpdateKeys = config.getBoolean("persistance.sql-keyvalue-store.update_key");

    return new SQLKeyValueStore<String>(host, port, database, user, password, canUpdateKeys,
        new Function<String, byte[]>() {
          @Override
          public byte[] apply(String arg0) {
            return arg0.getBytes();
          }
        }, new Function<byte[], String>() {
          @Override
          public String apply(byte[] arg0) {
            return new String(arg0);
          }
        });
  }

  @Test
  public void shouldStoreObjects() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    store.put("one", "value 1");
    store.put("two", "value 2");

    Assert.assertEquals("value 2", store.get("two"));
    Assert.assertEquals("value 1", store.get("one"));
    store.delete("one");
    store.delete("two");
    Assert.assertNull(store.get("one"));
    Assert.assertNull(store.get("two"));
  }

  @Test
  public void shouldReturnNullOnNonExistentKey() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    Assert.assertNull(store.get("bad key"));
  }

  @Test
  public void shouldPerformTransaction() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    store.put("one", "abc");
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          KeyValueStore<String> store2 = getKeyValueStore();
          store2.performTransaction("one", new Function<String, String>() {
            @Override
            public String apply(String value) {
              return value + "|" + value;
            }
          });
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    final Thread thread = new Thread(runnable);

    store.performTransaction("one", new Function<String, String>() {

      @Override
      public String apply(String value) {
        thread.start();
        try {
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          return null;
        }
        return value + value;
      }

    });
    thread.join();

    Assert.assertEquals("abcabc|abcabc", store.get("one"));
    store.delete("one");
    Assert.assertNull(store.get("one"));
  }

}
