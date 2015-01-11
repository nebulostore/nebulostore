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
 * @author Bolek Kulbabinski
 */
public final class CryptoUtils {
  private static Logger logger_ = Logger.getLogger(CryptoUtils.class);
  private static final String RSA_ALGORITHM = "RSA";
  private static final String AES_ALGORITHM = "AES";


  public static PublicKey readPublicKey(String filename) throws CryptoException {
    try {
      X509EncodedKeySpec x509EncodedKeySpec =
          new X509EncodedKeySpec(CryptoUtils.readBytes(filename));
      KeyFactory keyFactory = KeyFactory.getInstance(CryptoUtils.RSA_ALGORITHM);
      return keyFactory.generatePublic(x509EncodedKeySpec);
    } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger_.error(e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static PrivateKey readPrivateKey(String filename) throws CryptoException {
    try {
      PKCS8EncodedKeySpec pkcs8EncodedKeySpec =
          new PKCS8EncodedKeySpec(CryptoUtils.readBytes(filename));
      KeyFactory keyFactory = KeyFactory.getInstance(CryptoUtils.RSA_ALGORITHM);
      return keyFactory.generatePrivate(pkcs8EncodedKeySpec);
    } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      logger_.error(e);
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

  private static byte[] encrypt(byte[] message, Key key, String algorithm) throws CryptoException {
    try {
      Cipher cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.ENCRYPT_MODE, key);
      return cipher.doFinal(message);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException |
        IllegalBlockSizeException | BadPaddingException e) {
      logger_.error(e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  private static byte[] decrypt(byte[] cipherText, Key key, String algorithm)
      throws CryptoException {
    try {
      Cipher cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.DECRYPT_MODE, key);
      return cipher.doFinal(cipherText);
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException |
        IllegalBlockSizeException | BadPaddingException e) {
      logger_.error(e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  private static byte[] encryptRSA(byte[] message, Key key) throws CryptoException {
    return encrypt(message, key, RSA_ALGORITHM);
  }

  private static byte[] decryptRSA(byte[] message, Key key) throws CryptoException {
    return decrypt(message, key, RSA_ALGORITHM);
  }

  private static byte[] encryptAES(byte[] message, Key key) throws CryptoException {
    return encrypt(message, key, AES_ALGORITHM);
  }

  private static byte[] decryptAES(byte[] message, Key key) throws CryptoException {
    return decrypt(message, key, AES_ALGORITHM);
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

  public static String getRandomString() {
    return getRandomId().toString();
  }

  public static EncryptedObject encryptObject(Serializable object) throws CryptoException {
    return new EncryptedObject(serializeObject(object));
  }

  public static EncryptedObject encryptObject(Serializable object, Key key) throws CryptoException {
    Key secretKey = generateSecretKey();
    byte[] cipherText = encryptAES(serializeObject(object), secretKey);
    byte[] cipherKey = encryptRSA(serializeObject(secretKey), key);
    byte[] cipher = ArrayUtils.addAll(cipherKey, cipherText);
    return new EncryptedObject(cipher);
  }

  private static SecretKey generateSecretKey() throws CryptoException {
    try {
      KeyGenerator keyGen = KeyGenerator.getInstance(AES_ALGORITHM);
      keyGen.init(256);
      return keyGen.generateKey();
    } catch (NoSuchAlgorithmException e) {
      logger_.error(e);
      throw new CryptoException(e.getMessage(), e);
    }
  }

  public static Object decryptObject(EncryptedObject encryptedObject, Key key) throws
      CryptoException {
    byte[] cipherKey = Arrays.copyOfRange(encryptedObject.getEncryptedData(), 0, 512);
    byte[] cipherText = Arrays.copyOfRange(
        encryptedObject.getEncryptedData(), 512, encryptedObject.size());
    Key secretKey = (SecretKey) deserializeObject(decryptRSA(cipherKey, key));
    return deserializeObject(decryptAES(cipherText, secretKey));
  }

  public static Object decryptObject(EncryptedObject encryptedObject) throws
      CryptoException {
    return deserializeObject(encryptedObject.getEncryptedData());
  }

  public static byte[] encryptData(byte[] data) {
    return data;
  }

  public static byte[] decryptData(byte[] data) {
    return data;
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
    StringBuilder sb = new StringBuilder();
    for (byte b : array)
       sb.append(String.format("%02x", b & 0xff));
    return sb.toString();
  }

  public static String sha(EncryptedObject encryptedObject) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("SHA-1");
    } catch (NoSuchAlgorithmException e) {
      logger_.error(e.getMessage());
    }
    md.update(encryptedObject.getEncryptedData());
    return byteArrayToHexString(md.digest());
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
