package org.nebulostore.broker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;

/**
 * All persistent data held by Broker.
 *
 * @author szymonmatejczyk
 */
public final class BrokerContext {
  private static Logger logger_ = Logger.getLogger(BrokerContext.class);

  private final ContractsSet contracts_ = new ContractsSet();
  private final Map<CommAddress, List<Contract>> contractMap_ =
      new TreeMap<CommAddress, List<Contract>>();

  private final ReadWriteLock readWriteLock_ = new ReentrantReadWriteLock();
  private final Lock readLock_ = readWriteLock_.readLock();
  private final Lock writeLock_ = readWriteLock_.writeLock();

  /**
   * Available space for contract.
   */
  private Map<Contract, Integer> freeSpaceMap_ = new HashMap<Contract, Integer>();

  /**
   * Contract offers sent to peers, waiting for response.
   */
  private Map<CommAddress, Contract> contractOffers_ = new HashMap<CommAddress, Contract>();

  public void addContract(Contract contract) {
    writeLock_.lock();
    try {
      logger_.debug("Adding contract with: " + contract.getPeer().toString());
      contracts_.add(contract);
      if (contractMap_.containsKey(contract.getPeer())) {
        contractMap_.get(contract.getPeer()).add(contract);
      } else {
        List<Contract> contractList = new ArrayList<Contract>();
        contractList.add(contract);
        contractMap_.put(contract.getPeer(), contractList);
      }
    } finally {
      writeLock_.unlock();
    }
  }

  public void remove(Contract contract) {
    writeLock_.lock();
    try {
      logger_.debug("Removing contract with: " + contract.getPeer().toString());
      contracts_.remove(contract);
      if (contractMap_.containsKey(contract.getPeer())) {
        contractMap_.get(contract.getPeer()).remove(contract);
      } else {
        logger_.warn("Removing not existing contract with: " +
            contract.getPeer().toString());
      }
    } finally {
      writeLock_.unlock();
    }
  }

  public void addContractOffer(CommAddress recipient, Contract offer) {
    contractOffers_.put(recipient, offer);
  }

  public void removeOffer(CommAddress recipient) {
    contractOffers_.remove(recipient);
  }

  public boolean containsOffer(CommAddress recipient) {
    return contractOffers_.containsKey(recipient);
  }

  public int getNumberOfOffers() {
    return contractOffers_.size();
  }

  public ContractsSet acquireReadAccessToContracts() {
    readLock_.lock();
    return contracts_;
  }

  public void disposeReadAccessToContracts() {
    readLock_.unlock();
  }

  public List<Contract> getUserContracts(CommAddress id) {
    readLock_.lock();
    try {
      return contractMap_.get(id);
    } finally {
      readLock_.unlock();
    }
  }

  public int getNumberOfContractsWith(CommAddress id) {
    List<Contract> contracts = contractMap_.get(id);
    if (contracts == null) {
      return 0;
    } else {
      return contracts.size();
    }
  }

  public long getContractsRealSize() {
    readLock_.lock();
    try {
      return contracts_.realSize();
    } finally {
      readLock_.unlock();
    }
  }

  public CommAddress[] getReplicas() {
    readLock_.lock();
    try {
      CommAddress[] addresses = new CommAddress[contracts_.size()];
      Iterator<Contract> iter = contracts_.iterator();
      int i = 0;
      while (iter.hasNext()) {
        addresses[i] = iter.next().getPeer();
        i++;
      }
      return addresses;
    } finally {
      readLock_.unlock();
    }
  }

}
