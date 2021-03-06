package org.nebulostore.crypto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import com.google.common.base.Charsets;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;

/**
 * Library of cryptographic and serialization functions.
 *
 * @author Bolek Kulbabinski, lukaszsiczek
 */
public final class CryptoUtils {
  private static Logger logger_ = Logger.getLogger(CryptoUtils.class);
  private static final String RSA_ALGORITHM = "RSA";
  private static final String AES_ALGORITHM = "AES";
  private static final String DES_ALGORITHM = "DES/ECB/PKCS5Padding";
  private static final int SYMETRIC_KEY_BYTE_LENGTH = 256;
  private static final int ASYMETRIC_ENCRYPTION_BYTE_LENGTH = 512;
  private static final int ASYMETRIC_ENCRYPTION_KEY_LENGTH = 8 * ASYMETRIC_ENCRYPTION_BYTE_LENGTH;

  public static PublicKey readPublicKey(byte[] file) throws CryptoException {
    try {
      X509EncodedKeySpec x509EncodedKeySpec =
          new X509EncodedKeySpec(file);
      KeyFactory keyFactory = KeyFactory.getInstance(CryptoUtils.RSA_ALGORITHM);
      return keyFactory.generatePublic(x509EncodedKeySpec);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger_.error("Unable to read public key", e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static PublicKey readPublicKeyFromPath(String filename) throws CryptoException {
    try {
      return CryptoUtils.readPublicKey(CryptoUtils.readBytes(filename));
    } catch (IOException e) {
      logger_.error("Unable to read public key from file " + filename, e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static PrivateKey readPrivateKey(byte[] file) throws CryptoException {
    try {
      PKCS8EncodedKeySpec pkcs8EncodedKeySpec =
          new PKCS8EncodedKeySpec(file);
      KeyFactory keyFactory = KeyFactory.getInstance(CryptoUtils.RSA_ALGORITHM);
      return keyFactory.generatePrivate(pkcs8EncodedKeySpec);
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger_.error("Unable to read private key", e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static PrivateKey readPrivateKeyFromPath(String filename) throws CryptoException {
    try {
      return CryptoUtils.readPrivateKey(CryptoUtils.readBytes(filename));
    } catch (IOException e) {
      logger_.error("Unable to read private key from file " + filename, e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static KeyPair generateKeyPair() throws CryptoException {
    try {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
      keyPairGenerator.initialize(ASYMETRIC_ENCRYPTION_KEY_LENGTH);
      return keyPairGenerator.generateKeyPair();
    } catch (NoSuchAlgorithmException e) {
      logger_.error("Unable to generate key pair.", e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  private static byte[] readBytes(String filename) throws IOException {
    File file = new File(filename);
    FileInputStream fileInputStream = new FileInputStream(file);
    DataInputStream dataInputStream = new DataInputStream(fileInputStream);
    try {
      byte[] keyBytes = new byte[(int) file.length()];
      dataInputStream.readFully(keyBytes);
      return keyBytes;
    } finally {
      dataInputStream.close();
    }
  }

  private static byte[] transform(byte[] text, Key key,
      String algorithm, int mode) throws CryptoException {
    try {
      Cipher cipher = Cipher.getInstance(algorithm);
      cipher.init(mode, key);
      return cipher.doFinal(text);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException |
        IllegalBlockSizeException | BadPaddingException e) {
      logger_.error("Unable to transform message", e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  private static byte[] encryptRSA(byte[] message, Key key) throws CryptoException {
    return transform(message, key, RSA_ALGORITHM, Cipher.ENCRYPT_MODE);
  }

  private static byte[] decryptRSA(byte[] message, Key key) throws CryptoException {
    return transform(message, key, RSA_ALGORITHM, Cipher.DECRYPT_MODE);
  }

  private static byte[] encryptAES(byte[] message, Key key) throws CryptoException {
    return transform(message, key, AES_ALGORITHM, Cipher.ENCRYPT_MODE);
  }

  private static byte[] decryptAES(byte[] message, Key key) throws CryptoException {
    return transform(message, key, AES_ALGORITHM, Cipher.DECRYPT_MODE);
  }

  private static byte[] encryptDES(byte[] message, Key key) throws CryptoException {
    return transform(message, key, DES_ALGORITHM, Cipher.ENCRYPT_MODE);
  }

  private static byte[] decryptDES(byte[] message, Key key) throws CryptoException {
    return transform(message, key, DES_ALGORITHM, Cipher.DECRYPT_MODE);
  }

  /**
   * Create cryptographically secure 128-bit long positive BigInteger.
   */
  public static BigInteger getRandomId() {
    BigInteger id =  new BigInteger(128, RANDOM);
    if (id.compareTo(BigInteger.ZERO) == -1) {
      id = id.negate().subtract(new BigInteger("-1"));
    }
    return id;
  }

  public static BigInteger getRandomNumber(BigInteger m) {
    return getRandomId().mod(m);
  }

  public static String getRandomString() {
    return getRandomId().toString();
  }

  public static EncryptedObject encryptObject(Serializable object, Key key) throws CryptoException {
    Key secretKey = generateSecretKey();
    byte[] cipherText = encryptAES(serializeObject(object), secretKey);
    byte[] cipherKey = encryptRSA(serializeObject(secretKey), key);
    byte[] cipher = ArrayUtils.addAll(cipherKey, cipherText);
    return new EncryptedObject(cipher);
  }

  public static EncryptedObject encryptObjectWithSessionKey(Serializable object,
      SecretKey secretKey) throws CryptoException {
    return new EncryptedObject(encryptDES(serializeObject(object), secretKey));
  }

  public static Object decryptObjectWithSessionKey(EncryptedObject object,
      SecretKey secretKey) throws CryptoException {
    return deserializeObject(decryptDES(object.getEncryptedData(), secretKey));
  }

  public static EncryptedObject encryptObjectWithSecretKey(Serializable object,
      SecretKey secretKey) throws CryptoException {
    return new EncryptedObject(encryptAES(serializeObject(object), secretKey));
  }

  public static Object decryptObjectWithSecretKey(EncryptedObject object,
      SecretKey secretKey) throws CryptoException {
    return deserializeObject(decryptAES(object.getEncryptedData(), secretKey));
  }

  public static SecretKey generateSecretKey() throws CryptoException {
    try {
      KeyGenerator keyGen = KeyGenerator.getInstance(AES_ALGORITHM);
      keyGen.init(CryptoUtils.SYMETRIC_KEY_BYTE_LENGTH);
      return keyGen.generateKey();
    } catch (NoSuchAlgorithmException e) {
      logger_.error("Symetric key generation has failed", e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static Object decryptObject(EncryptedObject encryptedObject, Key key) throws
      CryptoException {
    byte[] cipherKey = Arrays.copyOfRange(
        encryptedObject.getEncryptedData(), 0, CryptoUtils.ASYMETRIC_ENCRYPTION_BYTE_LENGTH);
    byte[] cipherText = Arrays.copyOfRange(encryptedObject.getEncryptedData(),
        CryptoUtils.ASYMETRIC_ENCRYPTION_BYTE_LENGTH, encryptedObject.size());
    Key secretKey = (Key) deserializeObject(decryptRSA(cipherKey, key));
    return deserializeObject(decryptAES(cipherText, secretKey));
  }

  public static byte[] serializeObject(Serializable object) throws CryptoException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] result;
    try {
      ObjectOutput out = new ObjectOutputStream(baos);
      out.writeObject(object);
      result = baos.toByteArray();
      out.close();
      baos.close();
    } catch (IOException exception) {
      throw new CryptoException("IOError in serializing object.", exception);
    }
    return result;
  }

  public static Object deserializeObject(byte[] serializedObject) throws CryptoException {
    Object o;
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(serializedObject);
      ObjectInput in = new ObjectInputStream(bais);
      o = in.readObject();
      bais.close();
      in.close();
    } catch (IOException exception) {
      throw new CryptoException("IOError in deserializing object.", exception);
    } catch (ClassNotFoundException exception) {
      throw new CryptoException("Cannot deserialize object of unknown class.", exception);
    }
    return o;
  }

  private static String byteArrayToHexString(byte[] array) {
    return DatatypeConverter.printHexBinary(array);
  }

  private static byte[] hexStringToByteArray(String hexString) {
    return DatatypeConverter.parseHexBinary(hexString);
  }

  public static String sha(EncryptedObject encryptedObject) {
    return byteArrayToHexString(sha(encryptedObject.getEncryptedData()));
  }

  private static byte[] sha(byte[] data) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      logger_.error(e.getMessage());
    }
    md.update(data);
    return md.digest();
  }

  public static String generateMAC(Serializable object, Key key) throws CryptoException {
    byte[] sha = sha(serializeObject(object));
    return byteArrayToHexString(encryptRSA(sha, key));
  }

  public static boolean verifyMAC(Serializable object, String version, Key key)
      throws CryptoException {
    byte[] sha = sha(serializeObject(object));
    byte[] decryptedVersion = decryptRSA(hexStringToByteArray(version), key);
    return Arrays.equals(sha, decryptedVersion);
  }

  public static double nextDouble() {
    return RANDOM.nextDouble();
  }

  public static String objectToXml(Object object, boolean pretty, Class<?>... context)
      throws NebuloException {
    try {
      JAXBContext jaxbContext = JAXBContext.newInstance(context);
      Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
      jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, pretty);
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      jaxbMarshaller.marshal(object, stream);
      return new String(stream.toByteArray(), Charsets.UTF_8);
    } catch (JAXBException e) {
      throw new NebuloException("Unable to serialize", e);
    }
  }

  public static <T> T xmlToObject(String xml, Class<T> clazz) throws NebuloException {
    try {
      JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
      Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
      return (T) jaxbUnmarshaller.unmarshal(new ByteArrayInputStream(xml.getBytes(Charsets.UTF_8)));
    } catch (JAXBException e) {
      throw new NebuloException("Unable to deserialize", e);
    }
  }

  private static final SecureRandom RANDOM = new SecureRandom();

  private CryptoUtils() { }
}
