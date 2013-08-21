package org.nebulostore.broker;

import com.google.inject.Inject;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.address.CommAddress;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;


/**
 * Broker is always a singleton job. See BrokerMessageForwarder.
 * @author Bolek Kulbabinski
 */
public abstract class Broker extends JobModule {
  protected CommAddress myAddress_;

  protected NetworkMonitor networkMonitor_;

  @Inject
  private void setDependencies(CommAddress myAddress, NetworkMonitor networkMonitor) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
  }

}
