package org.nebulostore.broker;


/**
 * @author Piotr Malicki
 */
public class ReplicatorData {

  private final int sequentialNumber_;
  private final Contract contract_;

  public ReplicatorData(int sequentialNumber, Contract contract) {
    sequentialNumber_ = sequentialNumber;
    contract_ = contract;
  }

  public int getSequentialNumber() {
    return sequentialNumber_;
  }

  public Contract getContract() {
    return contract_;
  }

  @Override
  public String toString() {
    return "<" + contract_.getPeer() + ", " + sequentialNumber_ + ">";
  }

}
