package org.nebulostore.persistence;

import java.io.IOException;
import java.security.SecureRandom;

import com.google.common.base.Charsets;
import com.google.common.base.Function;

/**
 * @author Bolek Kulbabinski
 */
public class FileStoreTest extends KeyValueStoreTestTemplate {

  private static final String TEST_DIR = "temp/temp_test_dir" +  new SecureRandom().nextInt(100000);

  @Override
  protected KeyValueStore<String> getKeyValueStore() throws IOException {
    Function<String, byte[]> serializer = new Function<String, byte[]>() {

      @Override
      public byte[] apply(String value) {
        return value.getBytes(Charsets.UTF_8);
      }

    };
    Function<byte[], String> deserializer = new Function<byte[], String>() {

      @Override
      public String apply(byte[] data) {
        return new String(data, Charsets.UTF_8);
      }

    };
    return new FileStore<String>(TEST_DIR, serializer, deserializer);
  }

}
