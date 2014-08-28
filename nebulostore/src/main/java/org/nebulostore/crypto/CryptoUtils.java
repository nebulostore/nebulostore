package org.nebulostore.crypto;

import com.google.common.base.Charsets;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

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

  // TODO: Encryption must use cryptographic keys (add parameters?)
  public static EncryptedObject encryptObject(Serializable object) throws CryptoException {
    return new EncryptedObject(serializeObject(object));
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
  
  public static int nextInt(int n) {
    return RANDOM.nextInt(n);
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
