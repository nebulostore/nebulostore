package org.nebulostore.crypto.keys;

import java.security.Key;

import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI.KeyType;

/**
 * @author lukaszsiczek
 */
public class LocalDiscKeyHandler implements KeyHandler {

  private final String keyPath_;
  private final KeyType keyType_;

  public LocalDiscKeyHandler(String keyPath, KeyType keyType) {
    keyPath_ = keyPath;
    keyType_ = keyType;
  }

  @Override
  public Key load() throws CryptoException {
    switch (keyType_) {
      case PUBLIC:
        return CryptoUtils.readPublicKey(keyPath_);
      case PRIVATE:
        return CryptoUtils.readPrivateKey(keyPath_);
      default:
        break;
    }
    throw new CryptoException("Key type error");
  }

}
