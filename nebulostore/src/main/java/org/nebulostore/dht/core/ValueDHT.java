package org.nebulostore.dht.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;

/**
 * @author marcin
 */

public class ValueDHT implements Serializable {
  private static Logger logger_ = Logger.getLogger(ValueDHT.class);
  private static final long serialVersionUID = 8747517245988476609L;
  private final Mergeable value_;

  public Mergeable getValue() {
    return value_;
  }

  public ValueDHT(Mergeable v) {
    value_ = v;
  }

  public String serializeValue() {
    // TODO: Move it to utils.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(baos);
      oos.writeObject(value_);
    } catch (IOException e) {
      logger_.warn("IOException when serializing value: " + e);
    }

    String serialized = new String(Base64.encode(baos.toByteArray()),
            Charset.forName("UTF-8"));
    return serialized;
  }

  public static ValueDHT build(String serialized) {
    byte[] data = Base64.decode(serialized);
    ByteArrayInputStream baos = new ByteArrayInputStream(data);
    ObjectInputStream ois = null;
    try {
      ois = new ObjectInputStream(baos);
      return new ValueDHT((Mergeable) ois.readObject());
    } catch (IOException e) {
      logger_.warn("IOException when serializing value: " + e);
    } catch (ClassNotFoundException e) {
      logger_.warn("ClassNotFoundException when serializing value: " + e);
    }

    return null;
  }

  @Override
  public String toString() {
    return value_.toString();
  }
}
