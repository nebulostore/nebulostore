package org.nebulostore.crypto.dh;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.KeyAgreement;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.interfaces.DHPublicKey;
import javax.crypto.spec.DESKeySpec;
import javax.crypto.spec.DHParameterSpec;

import org.nebulostore.crypto.CryptoException;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 * Diffie-Hellman Protocol:
 * - 1st step: Alice generates DHParams, KeyPair and sends (DHParams, PublicKey) to Bob
 * - 2nd step: Bob generate KeyPair using DHParams, sends PublicKey
 *             and finish KeyAgreement using Alice PublicKey
 * - 3rd step: Alice finishes KeyAgreement using Bob PublicKey
 * - 4th step: Both Alice and Bob generate AES SecretKey using established Key
 */
public final class DiffieHellmanProtocol {

  private static final String DIFFIE_HELLMAN_ALGORITHM = "DH";
  private static final String DES_ALGORITHM = "DES";
  private static final int DIFFIE_HELLMAN_KEY_SIZE = 1024;

  private DiffieHellmanProtocol() {
  }

  public static Pair<KeyAgreement, DiffieHellmanInitPackage> firstStepDHKeyAgreement()
      throws CryptoException {
    try {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      keyPairGenerator.initialize(DIFFIE_HELLMAN_KEY_SIZE);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      DHParameterSpec dhSpec = ((DHPublicKey) keyPair.getPublic()).getParams();
      DiffieHellmanInitPackage diffieHellmanPackage = new DiffieHellmanInitPackage(dhSpec.getP(),
          dhSpec.getG(), dhSpec.getL(), keyPair.getPublic().getEncoded());

      KeyAgreement keyAgreement = KeyAgreement.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      keyAgreement.init(keyPair.getPrivate());
      return new Pair<KeyAgreement, DiffieHellmanInitPackage>(keyAgreement, diffieHellmanPackage);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static Pair<KeyAgreement, DiffieHellmanResponsePackage> secondStepDHKeyAgreement(
      DiffieHellmanInitPackage diffieHellmanInitPackage) throws CryptoException {
    try {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      DHParameterSpec dhSpec = new DHParameterSpec(
          diffieHellmanInitPackage.getDHParamP(),
          diffieHellmanInitPackage.getDHParamG(),
          diffieHellmanInitPackage.getDHParamL());
      keyPairGenerator.initialize(dhSpec);
      KeyPair keyPair = keyPairGenerator.generateKeyPair();

      KeyAgreement keyAgreement = KeyAgreement.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      keyAgreement.init(keyPair.getPrivate());

      KeyFactory keyFactory = KeyFactory.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      X509EncodedKeySpec x509EncodedKeySpec =
          new X509EncodedKeySpec(diffieHellmanInitPackage.getPublicKey());
      PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec);
      keyAgreement.doPhase(publicKey, true);

      DiffieHellmanResponsePackage diffieHellmanResponsePackage =
          new DiffieHellmanResponsePackage(keyPair.getPublic().getEncoded());
      return new Pair<KeyAgreement, DiffieHellmanResponsePackage>(
          keyAgreement, diffieHellmanResponsePackage);
    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException |
          InvalidKeyException | InvalidKeySpecException e) {
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static KeyAgreement thirdStepDHKeyAgreement(KeyAgreement keyAgreement,
      DiffieHellmanResponsePackage diffieHellmanResponsePackage) throws CryptoException {
    try {
      KeyFactory keyFactory = KeyFactory.getInstance(DIFFIE_HELLMAN_ALGORITHM);
      X509EncodedKeySpec x509EncodedKeySpec =
          new X509EncodedKeySpec(diffieHellmanResponsePackage.getPublicKey());
      PublicKey publicKey = keyFactory.generatePublic(x509EncodedKeySpec);
      keyAgreement.doPhase(publicKey, true);
      return keyAgreement;
    } catch (NoSuchAlgorithmException | InvalidKeySpecException | InvalidKeyException
        | IllegalStateException e) {
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static SecretKey fourthStepDHKeyAgreement(KeyAgreement keyAgreement)
      throws CryptoException {
    try {
      SecretKeyFactory skf = SecretKeyFactory.getInstance(DES_ALGORITHM);
      DESKeySpec desSpec = new DESKeySpec(keyAgreement.generateSecret());
      return skf.generateSecret(desSpec);
    } catch (InvalidKeyException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new CryptoException(e.getMessage(), e);
    }
  }
}
