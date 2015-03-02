package org.nebulostore.crypto.dh;

import java.io.Serializable;
import java.math.BigInteger;

/**
 * @author lukaszsiczek
 */
public class DiffieHellmanInitPackage implements Serializable {

  private static final long serialVersionUID = 5487724761312386719L;

  private BigInteger dhParamP_;
  private BigInteger dhParamG_;
  private int dhParamL_;

  private byte[] publicKey_;

  public DiffieHellmanInitPackage(BigInteger dhParamP, BigInteger dhParamG,
      int dhParamL, byte[] publicKey) {
    dhParamP_ = dhParamP;
    dhParamG_ = dhParamG;
    dhParamL_ = dhParamL;
    publicKey_ = publicKey;
  }

  public BigInteger getDHParamP() {
    return dhParamP_;
  }

  public BigInteger getDHParamG() {
    return dhParamG_;
  }

  public int getDHParamL() {
    return dhParamL_;
  }

  public byte[] getPublicKey() {
    return publicKey_;
  }

}
