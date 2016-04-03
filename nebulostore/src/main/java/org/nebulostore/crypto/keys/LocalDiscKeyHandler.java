package org.nebulostore.crypto.keys;

import java.security.Key;

import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI.KeyType;

/**
 * @author lukaszsiczek
 */
public class LocalDiscKeyHandler implements KeyHandler {

  private String keyPath_;
  private KeyType keyType_;
  private Key key_;

  public LocalDiscKeyHandler(String keyPath, KeyType keyType) {
    keyPath_ = keyPath;
    keyType_ = keyType;
  }

  public LocalDiscKeyHandler(Key key) {
    key_ = key;
  }

  @Override
  public Key load() throws CryptoException {
    if (key_ != null) {
      return key_;
    }
    switch (keyType_) {
      case PUBLIC:
        key_ = CryptoUtils.readPublicKeyFromPath(keyPath_);
        return key_;
      case PRIVATE:
        key_ = CryptoUtils.readPrivateKeyFromPath(keyPath_);
        return key_;
      default:
        break;
    }
    throw new CryptoException("Key type error");
  }

}
