package org.nebulostore.systest.newcommunication.functionaltest;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Controller to which test instances should register and notify about events.
 *
 * @author Grzegorz Milka
 *
 */
public interface RemoteCommunicationFacadeController extends Remote {
  void registerRemoteCommunicationFacade(RemoteCommunicationFacadeAdapter commFacade)
      throws RemoteException;
  void notifyAboutMessage(CommMessage message) throws RemoteException;
  void notifyAboutPeerFound(CommAddress source, CommAddress newPeer) throws RemoteException;
}
