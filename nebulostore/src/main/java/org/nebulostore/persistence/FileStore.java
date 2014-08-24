package org.nebulostore.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import com.google.common.base.Function;
import com.google.common.io.Files;
import org.nebulostore.utils.LockMap;

/**
 * Store data as files in a given directory. Key is used as the file name and value is stored as the
 * file contents.
 *
 * @author Bolek Kulbabinski
 *
 */
public class FileStore<T> implements KeyValueStore<T> {

  private final String rootDir_;
  private final Function<T, byte[]> serializer_;
  private final Function<byte[], T> deserializer_;
  private final LockMap lockMap_;


  public FileStore(String rootDir, Function<T, byte[]> serializer,
      Function<byte[], T> deserializer) throws IOException {
    rootDir_ = rootDir;
    serializer_ = serializer;
    deserializer_ = deserializer;
    lockMap_ = new LockMap();
    Files.createParentDirs(new File(getFileName("any_file")));
  }

  @Override
  public void put(String key, T value) throws IOException {
    byte[] data = serializer_.apply(value);
    lockMap_.lock(key);
    try {
      doWrite(key, data);
    } catch (IOException e) {
      throw new IOException("Unable to save data", e);
    } finally {
      lockMap_.unlock(key);
    }
  }

  @Override
  public T get(String key) {
    lockMap_.lock(key);
    try {
      return deserializer_.apply(doRead(key));
    } catch (IOException e) {
      return null;
    } finally {
      lockMap_.unlock(key);
    }
  }

  @Override
  public void delete(String key) {
    lockMap_.lock(key);
    try {
      doDelete(key);
    } finally {
      lockMap_.unlock(key);
    }
  }

  @Override
  public void performTransaction(String key, Function<T, T> function)
      throws IOException {
    lockMap_.lock(key);
    try {
      T oldValue;
      try {
        oldValue = deserializer_.apply(doRead(key));
      } catch (IOException e) {
        oldValue = null;
      }
      doWrite(key, serializer_.apply(function.apply(oldValue)));
    } catch (IOException e) {
      throw new IOException("Unable to perform transaction", e);
    } finally {
      lockMap_.unlock(key);
    }
  }



  private byte[] doRead(String key) throws IOException {
    return Files.asByteSource(new File(getFileName(key))).read();
  }

  private void doWrite(String key, byte[] data) throws IOException {
    File file = new File(getFileName(key));
    file.createNewFile();
    Files.asByteSink(file).write(data);
  }

  private void doDelete(String key) {
    new File(getFileName(key)).delete();
  }

  private String getFileName(String key) {
    return Paths.get(rootDir_, key).toString();
  }
}
