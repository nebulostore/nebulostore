package org.nebulostore.crypto.keys;

import java.security.Key;

import org.nebulostore.crypto.CryptoException;

/**
 * @author lukaszsiczek
 */
public interface KeyHandler {

  Key load() throws CryptoException;

}
