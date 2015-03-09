package org.nebulostore.systest;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.NetworkMonitorImpl;
import org.nebulostore.networkmonitor.RandomPeersGossipingModule;
import org.nebulostore.systest.messages.ChangeTestMessageHandlerMessage;
import org.nebulostore.timer.Timer;

/**
 * NetworkMonitor used for testing - handling additional messages.
 *
 * @author szymonmatejczyk
 *
 */
public class NetworkMonitorForTesting extends NetworkMonitorImpl {

  @Inject
  public NetworkMonitorForTesting(
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      CommAddress commAddress,
      Timer timer,
      Provider<RandomPeersGossipingModule> randomPeersGossipingModuleProvider,
      Provider<ConnectionTestMessageHandler> connectionTestMessageHandlerProvider,
      @Named(CONFIGURATION_PREFIX + "statistics-update-interval-millis")
      long statisticsUpdateIntervalMillis,
      EncryptionAPI encryptionAPI) {
    super(dispatcherQueue, commAddress, timer, randomPeersGossipingModuleProvider,
        connectionTestMessageHandlerProvider, statisticsUpdateIntervalMillis, encryptionAPI);
    visitor_ = new NetworkMonitorForTestingVisitor();
  }

  public class NetworkMonitorForTestingVisitor extends NetworkMonitorVisitor {

    public void visit(ChangeTestMessageHandlerMessage message) {
      connectionTestMessageHandlerProvider_ = message.getProvider();
    }

  }

}
