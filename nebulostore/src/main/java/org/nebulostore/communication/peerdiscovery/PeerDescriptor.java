package org.nebulostore.communication.peerdiscovery;

import java.io.Serializable;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Grzegorz Milka
 */
public class PeerDescriptor implements Serializable {
  private static final long serialVersionUID = -5313861167029739516L;
  private final CommAddress peerAddress_;
  private int age_;

  public PeerDescriptor(CommAddress peerAddress) {
    peerAddress_ = peerAddress;
    age_ = 0;
  }

  /**
   * Equal iff address is equal.
   */
  @Override
  public boolean equals(Object o) {
    return (o instanceof PeerDescriptor) &&
      (peerAddress_.equals(((PeerDescriptor) o).getPeerAddress()));
  }

  public int getAge() {
    return age_;
  }

  public CommAddress getPeerAddress() {
    return peerAddress_;
  }

  @Override
  public int hashCode() {
    return peerAddress_.hashCode();
  }

  public void setAge(int age) {
    age_ = age;
  }

  @Override
  public String toString() {
    return "PeerDescriptor with address: " + getPeerAddress() +
      ", age: " + getAge();
  }
}
