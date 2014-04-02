package org.nebulostore.communication;

import java.io.IOException;
import java.util.Collection;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.communication.bootstrap.BootstrapService;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.AddressMappingMaintainer;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.naming.CommAddressResolver;
import org.nebulostore.communication.netutils.NetworkAddressDiscovery;
import org.nebulostore.communication.netutils.remotemap.RemoteMapFactory;
import org.nebulostore.communication.peerdiscovery.PeerDiscovery;
import org.nebulostore.communication.peerdiscovery.PeerDiscoveryFactory;
import org.nebulostore.communication.routing.ByteListenerService;
import org.nebulostore.communication.routing.ByteSender;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.communication.routing.MessageMatcher;
import org.nebulostore.communication.routing.MessageSendFuture;
import org.nebulostore.communication.routing.Router;
import org.nebulostore.communication.routing.SendResult;


/**
 * Facade for communication module.
 *
 * @author Grzegorz Milka
 *
 */
public class CommunicationFacade {
  private static final Logger LOGGER = Logger.getLogger(CommunicationFacade.class);
  private final PeerDiscoveryFactory peerDiscoveryFactory_;

  private final ByteSender byteSender_;
  private final ByteListenerService byteListener_;

  private final Router router_;

  private final AddressMappingMaintainer amMaintainer_;
  private final RemoteMapFactory remoteMapFactory_;

  private final BootstrapService bootstrapService_;

  private final NetworkAddressDiscovery netAddrDiscovery_;

  private final CommAddressResolver resolver_;

  private final ExecutorService serviceExecutor_;

  private PeerDiscovery peerDiscovery_;

  @Inject
  public CommunicationFacade(ByteSender byteSender,
      ByteListenerService byteListener,
      Router router,
      AddressMappingMaintainer amMaintainer,
      PeerDiscoveryFactory peerDiscoveryFactory,
      NetworkAddressDiscovery netAddrDiscovery,
      RemoteMapFactory remoteMapFactory,
      BootstrapService bootstrapService,
      CommAddressResolver resolver,
      @Named("communication.service-executor") ExecutorService serviceExecutor) {
    byteSender_ = byteSender;
    byteListener_ = byteListener;
    router_ = router;
    amMaintainer_ = amMaintainer;
    remoteMapFactory_ = remoteMapFactory;
    peerDiscoveryFactory_ = peerDiscoveryFactory;
    netAddrDiscovery_ = netAddrDiscovery;
    bootstrapService_ = bootstrapService;
    resolver_ = resolver;
    serviceExecutor_ = serviceExecutor;
  }

  public void addMessageListener(MessageMatcher matcher, MessageListener listener) {
    router_.addMessageListener(matcher, listener);
  }

  public void addPeerFoundListener(Observer o) {
    peerDiscovery_.addObserver(o);
  }

  public void removeMessageListener(MessageListener listener) {
    router_.removeMessageListener(listener);
  }

  public void removePeerFoundListener(Observer o) {
    peerDiscovery_.deleteObserver(o);
  }

  /**
   * Sends given CommMessage.
   *
   * @see Router
   *
   * @param message
   * @return
   */
  public MessageSendFuture sendMessage(CommMessage message) {
    return router_.sendMessage(message);
  }

  public MessageSendFuture sendMessage(CommMessage message, BlockingQueue<SendResult> results) {
    return router_.sendMessage(message, results);
  }

  public void startUp() throws IOException {
    LOGGER.info("startUp()");
    /* bootstrap */
    byteListener_.startUp();
    byteSender_.startUp();
    Collection<CommAddress> bootstrapCommAddresses = null;
    bootstrapService_.startUp();

    bootstrapCommAddresses = bootstrapService_.getBootstrapInformation().
        getBootstrapCommAddresses();

    netAddrDiscovery_.startUp();

    remoteMapFactory_.startUp();

    amMaintainer_.startUp();

    router_.startUp();

    peerDiscovery_ = peerDiscoveryFactory_.newPeerDiscovery(bootstrapCommAddresses);
    peerDiscovery_.startUp();

  }

  public void shutDown() throws InterruptedException {
    LOGGER.info("shutDown()");
    peerDiscovery_.shutDown();
    router_.shutDown();
    resolver_.shutDown();
    amMaintainer_.shutDown();
    remoteMapFactory_.shutDown();
    netAddrDiscovery_.shutDown();
    bootstrapService_.shutDown();
    serviceExecutor_.shutdown();
    byteSender_.shutDown();
    byteListener_.shutDown();
    LOGGER.info("shutDown(): void");
  }
}
