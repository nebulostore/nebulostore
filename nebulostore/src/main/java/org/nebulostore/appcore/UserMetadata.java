package org.nebulostore.appcore;

import java.io.Serializable;
import java.security.Key;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.dht.core.Mergeable;

/**
 * UserMetadata object is stored in main DHT. It contains data necessary for system,
 * that cannot be stored in as other files, because they are used for replica
 * management. It also stores user's ContractList.
 * @author szymonmatejczyk
 * @author bolek
 */
public class UserMetadata implements Mergeable, Serializable, PublicKeyMetadata {
  private static final long serialVersionUID = 8900375455728664721L;

  /* Id of user, that this metadata applies to. */
  private final AppKey owner_;

  private ContractList contractList_;

  private Key userPublicKey_;

  public UserMetadata(AppKey owner) {
    owner_ = owner;
  }

  public UserMetadata(AppKey owner, ContractList contractList) {
    this(owner);
    contractList_ = contractList;
  }

  public AppKey getOwner() {
    return owner_;
  }

  public ContractList getContractList() {
    return contractList_;
  }

  @Override
  public String toString() {
    return "Metadata [ owner : ( " + owner_ + " ), contractList: ( " +
        contractList_ + " ) ]";
  }

  @Override
  public Mergeable merge(Mergeable other) {
    // TODO: Implement this properly,
    //       Maybe composite objects also mergeable?
    if (other instanceof UserMetadata) {
      UserMetadata otherMetadata = (UserMetadata) other;
      if (userPublicKey_ == null) {
        userPublicKey_ = otherMetadata.userPublicKey_;
      }
      if (contractList_ == null) {
        contractList_ = otherMetadata.contractList_;
      }
    }
    return this;
  }

  @Override
  public Key getPublicKey() {
    return userPublicKey_;
  }

  @Override
  public void setPublicKey(Key key) {
    userPublicKey_ = key;
  }

}
