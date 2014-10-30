package org.nebulostore.appcore.model;

import java.io.Serializable;
import java.math.BigInteger;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Element of a list (currently raw data or NebuloAddress).
 * @author bolek
 */
/*
 * This is a new version of NebuloElement data structure required by new NebuloListAPI.
 * Some of the below methods/attributes for now are exact same methods as in the old version of ListAPI.
 */
public class NebuloElement implements Serializable, Comparable<NebuloElement> {
  private static final long serialVersionUID = -3926287447372574421L;

  // Element's metadata: timestamp, unique ID, author ID, author's signature.

  // Unique ID and timestamp are used for consistency purposes.
  protected BigInteger elementId_;
  protected long timestamp_;

  // Author's ID necessary for ...
  protected BigInteger authorId_;

  // Author's signature (needed to verify byzantine behavior of replicas).
  protected byte[] signature;

  // Content: either small amount of data or address of another NebuloObject.
  protected NebuloAddress address_;
  // This inner content is encrypted by default.
  protected EncryptedObject innerObject_;

  /**
   * Creates a new link to existing NebuloObject denoted by address.
   * 
   * @param address
   *          address of NebuloObject that this NebuloElement will point to
   */
  public NebuloElement(NebuloAddress address) {
    address_ = address;
    elementId_ = CryptoUtils.getRandomId();
    timestamp_ = System.currentTimeMillis();
  }

  /**
   * Creates a new link to existing NebuloObject.
   * 
   * @param object
   *          object that this NebuloElement refers to
   */
  public NebuloElement(NebuloObject object) {
    address_ = object.getAddress();
    elementId_ = CryptoUtils.getRandomId();
  }

  /**
   * Creates a new element containing some data of unknown structure.
   * 
   * @param object
   *          data to hold
   */
  public NebuloElement(EncryptedObject object) {
    innerObject_ = object;
    elementId_ = CryptoUtils.getRandomId();
  }

  public boolean isLink() {
    return address_ != null;
  }

  public NebuloAddress getAddress() {
    return address_;
  }

  public EncryptedObject getData() {
    return innerObject_;
  }

  public ObjectId getObjectId() {
    return address_.getObjectId();
  }

  @Override
  public int compareTo(NebuloElement nebuloElement) {
    return Long.compare(this.timestamp_, nebuloElement.timestamp_);
  }
}