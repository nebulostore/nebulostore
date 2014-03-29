package org.nebulostore.systest.newcommunication.functionaltest.messageexchange;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Collection;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Interface for test clients get initial test data and send results.
 *
 * @author Grzegorz Milka
 *
 */
public interface TestController extends Remote {
  /**
   * Registers client with given address and on test start it sends collection of
   * {@link CommAddress} of hosts participating in the test.
   *
   * @param address
   * @return
   * @throws RemoteException
   */
  Collection<CommAddress> registerClient(CommAddress address) throws RemoteException;

  /**
   * @param commAddress Address of the reporting node.
   * @param result
   */
  void sendResult(CommAddress commAddress, TestResult result) throws RemoteException;
}
