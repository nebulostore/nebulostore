package org.nebulostore.broker;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Contract between peers.
 * @author szymonmatejczyk
 */
@XmlRootElement
public class Contract implements Serializable {
  private static final long serialVersionUID = 8104248584725231818L;

  public static final int DEFAULT_CONTRACT_SIZE_KB = 10 * 1024;

  private final String contractId_;
  private CommAddress localPeerAddress_;
  private CommAddress remotePeerAddress_;
  private final int sizeKb_;

  public Contract(CommAddress localPeerAddress, CommAddress remotePeerAddress, int sizeKb) {
    localPeerAddress_ = localPeerAddress;
    remotePeerAddress_ = remotePeerAddress;
    sizeKb_ = sizeKb;

    contractId_ = CryptoUtils.getRandomString();
  }

  public Contract toLocalAndRemoteSwapped() {
    CommAddress tmp = localPeerAddress_;
    localPeerAddress_ = remotePeerAddress_;
    remotePeerAddress_ = tmp;
    return this;
  }

  public String getContractId() {
    return contractId_;
  }

  public CommAddress getPeer() {
    return remotePeerAddress_;
  }

  public int getSize() {
    return sizeKb_;
  }

  @Override
  public int hashCode() {
    return contractId_.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Contract other = (Contract) obj;
    if (contractId_ == null) {
      if (other.contractId_ != null) {
        return false;
      }
    } else if (!contractId_.equals(other.contractId_)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Contract [contractId=" +
        contractId_ + ", localPeerId_=" + localPeerAddress_ + ", remotePeerId_=" +
        remotePeerAddress_ + "]";
  }
}
