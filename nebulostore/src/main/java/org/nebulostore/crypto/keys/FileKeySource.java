package org.nebulostore.crypto.keys;

import org.nebulostore.crypto.EncryptionAPI.KeyType;

/**
 * @author lukaszsiczek
 */
public class FileKeySource implements KeySource {

  private String keyFilePath_;
  private KeyType keyType_;

  public FileKeySource(String keyFilePath, KeyType keyType) {
    keyFilePath_ = keyFilePath;
    keyType_ = keyType;
  }

  @Override
  public KeyHandler getKeyHandler() {
    return new LocalDiscKeyHandler(keyFilePath_, keyType_);
  }

  @Override
  public String toString() {
    return String.format("FileKeySource: filepath %s, keytype %s", keyFilePath_, keyType_);
  }
}
