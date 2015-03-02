package org.nebulostore.crypto.dh;

import java.io.Serializable;

/**
 * @author lukaszsiczek
 */
public class DiffieHellmanResponsePackage implements Serializable {

  private static final long serialVersionUID = -7383982725344229743L;
  private byte[] publicKey_;

  public DiffieHellmanResponsePackage(byte[] publicKey) {
    publicKey_ = publicKey;
  }

  public byte[] getPublicKey() {
    return publicKey_;
  }
}
