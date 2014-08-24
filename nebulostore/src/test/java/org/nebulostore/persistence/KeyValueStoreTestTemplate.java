package org.nebulostore.persistence;

import java.io.IOException;

import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Bolek Kulbabinski
 */
public abstract class KeyValueStoreTestTemplate {

  protected abstract KeyValueStore<String> getKeyValueStore() throws IOException;

  @Test
  public void shouldStoreObjects() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    store.put("one", "value 1");
    store.put("two", "value 2");

    Assert.assertEquals("value 2", store.get("two"));
    Assert.assertEquals("value 1", store.get("one"));
  }

  @Test
  public void shouldReturnNullOnNonExistentKey() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    Assert.assertNull(store.get("bad key"));
  }

  @Test
  public void shouldPerformTransaction() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    store.put("three", "abc");
    store.performTransaction("three", new Function<String, String>() {

      @Override
      public String apply(String value) {
        return value + value;
      }

    });

    Assert.assertEquals("abcabc", store.get("three"));
  }

  @Test
  public void shouldPerformTransactionOnNullObject() throws Exception {
    KeyValueStore<String> store = getKeyValueStore();
    store.performTransaction("four", new Function<String, String>() {

      @Override
      public String apply(String value) {
        return value == null ? "abcd" : value;
      }

    });

    Assert.assertEquals("abcd", store.get("four"));
  }
}
