package org.nebulostore.peers;

import java.math.BigInteger;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.RegisterInstanceInDHTModule;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.async.AsyncMessagingModule;
import org.nebulostore.async.checker.MessageReceivingCheckerModule;
import org.nebulostore.async.synchrogroup.SynchroPeerSetChangeSequencerModule;
import org.nebulostore.broker.Broker;
import org.nebulostore.communication.CommunicationPeerFactory;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.EncryptionAPI.KeyType;
import org.nebulostore.crypto.keys.DHTKeyHandler;
import org.nebulostore.crypto.keys.LocalDiscKeyHandler;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dispatcher.Dispatcher;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.rest.RestModule;
import org.nebulostore.rest.RestModuleImpl;
import org.nebulostore.timer.Timer;

/**
 * This is a regular peer with full functionality. It creates, connects and runs all modules.
 * To create a different peer, subclass Peer and set its class name in configuration.
 *
 * To customize the Peer, please override initializeModules(), runActively() and cleanModules().
 *
 * @author Bolek Kulbabinski
 */
public class Peer extends AbstractPeer {
  private static Logger logger_ = Logger.getLogger(Peer.class);

  protected Thread dispatcherThread_;
  protected Thread networkThread_;
  protected AsyncMessagingModule asyncMessagingModule_;
  protected SynchroPeerSetChangeSequencerModule synchroUpdateSequencer_;
  protected MessageReceivingCheckerModule msgReceivingChecker_;
  protected Thread msgReceivingCheckerThread_;

  protected BlockingQueue<Message> dispatcherInQueue_;
  protected BlockingQueue<Message> networkInQueue_;
  protected BlockingQueue<Message> commPeerInQueue_;
  protected BlockingQueue<Message> commPeerOutQueue_;

  protected AppKey appKey_;
  protected Broker broker_;
  protected Injector injector_;
  protected CommAddress commAddress_;
  protected Timer peerTimer_;
  protected NetworkMonitor networkMonitor_;

  private CommunicationPeerFactory commPeerFactory_;

  private int registrationTimeout_;

  private Thread restThread_;
  private boolean isRestEnabled_;
  private RestModule restModule_;

  private EncryptionAPI encryption_;
  private String instancePublicKeyPath_;
  private String instancePrivateKeyPath_;
  private String instancePublicKeyId_;
  private String instancePrivateKeyId_;
  private String userPublicKeyPath_;
  private String userPrivateKeyPath_;
  private String userPublicKeyId_;
  private String userPrivateKeyId_;

  @Inject
  public void setDependencies(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherInQueue,
                              @Named("NetworkQueue") BlockingQueue<Message> networkQueue,
                              @Named("CommunicationPeerInQueue")
                                BlockingQueue<Message> commPeerInQueue,
                              @Named("CommunicationPeerOutQueue")
                                BlockingQueue<Message> commPeerOutQueue,
                              Broker broker,
                              AppKey appKey,
                              CommAddress commAddress,
                              CommunicationPeerFactory commPeerFactory,
                              Timer timer,
                              NetworkMonitor networkMonitor,
                              Injector injector,
                              @Named("peer.registration-timeout") int registrationTimeout,
                              AsyncMessagingModule asyncMessagingModule,
                              SynchroPeerSetChangeSequencerModule synchroUpdateSequencer,
                              MessageReceivingCheckerModule msgReceivingChecker,
                              RestModuleImpl restModule,
                              @Named("rest-api.enabled") boolean isRestEnabled,
                              EncryptionAPI encryption,
                              @Named("security.instance.public-key") String instancePublicKeyPath,
                              @Named("security.instance.private-key") String instancePrivateKeyPath,
                              @Named("InstancePublicKeyId") String instancePublicKeyId,
                              @Named("InstancePrivateKeyId") String instancePrivateKeyId,
                              @Named("security.user.public-key") String userPublicKeyPath,
                              @Named("security.user.private-key") String userPrivateKeyPath,
                              @Named("UserPublicKeyId") String userPublicKeyId,
                              @Named("UserPrivateKeyId") String userPrivateKeyId) {
    dispatcherInQueue_ = dispatcherInQueue;
    networkInQueue_ = networkQueue;
    commPeerInQueue_ = commPeerInQueue;
    commPeerOutQueue_ = commPeerOutQueue;
    broker_ = broker;
    appKey_ = appKey;
    commAddress_ = commAddress;
    commPeerFactory_ = commPeerFactory;
    peerTimer_ = timer;
    networkMonitor_ = networkMonitor;
    injector_ = injector;
    registrationTimeout_ = registrationTimeout;
    asyncMessagingModule_ = asyncMessagingModule;
    synchroUpdateSequencer_ = synchroUpdateSequencer;
    msgReceivingChecker_ = msgReceivingChecker;
    isRestEnabled_ = isRestEnabled;
    encryption_ = encryption;
    instancePublicKeyPath_ = instancePublicKeyPath;
    instancePrivateKeyPath_ = instancePrivateKeyPath;
    instancePublicKeyId_ = instancePublicKeyId;
    instancePrivateKeyId_ = instancePrivateKeyId;
    userPublicKeyPath_ = userPublicKeyPath;
    userPrivateKeyPath_ = userPrivateKeyPath;
    userPublicKeyId_ = userPublicKeyId;
    userPrivateKeyId_ = userPrivateKeyId;

    // Create core threads.
    Runnable dispatcher = new Dispatcher(dispatcherInQueue_, networkInQueue_, injector_);
    dispatcherThread_ = new Thread(dispatcher, "Dispatcher");
    Runnable commPeer = commPeerFactory_.newCommunicationPeer(commPeerInQueue_, commPeerOutQueue_);
    networkThread_ = new Thread(commPeer, "CommunicationPeer");
    if (isRestEnabled_) {
      restModule_ = restModule;
      restThread_ = new Thread(restModule_, "Rest Thread");
    }
    msgReceivingCheckerThread_ = new Thread(msgReceivingChecker_, "Messages receiving checker");
  }

  @Override
  public void quitNebuloStore() {
    logger_.info("Started quitNebuloStore().");
    if (msgReceivingChecker_ != null) {
      msgReceivingChecker_.getInQueue().add(new EndModuleMessage());
    }
    if (asyncMessagingModule_ != null) {
      asyncMessagingModule_.getInQueue().add(new EndModuleMessage());
    }
    if (dispatcherInQueue_ != null) {
      dispatcherInQueue_.add(new EndModuleMessage());
    }
    if (isRestEnabled_) {
      restModule_.shutDown();
    }
    logger_.info("Finished quitNebuloStore().");
  }

  @Override
  public final void run() {
    runPeer();
  }

  private void runPeer() {
    initializeModules();
    startCoreThreads();
    runActively();
    joinCoreThreads();
    cleanModules();
  }

  /**
   * Puts replication group under appKey_ in DHT and InstanceMetadata under commAddress_.
   * Register peer public and private keys.
   *
   * @param appKey
   */
  protected void register(AppKey appKey) {
    RegisterInstanceInDHTModule registerInstanceMetadataModule = new RegisterInstanceInDHTModule();
    registerInstanceMetadataModule.setDispatcherQueue(dispatcherInQueue_);
    registerInstanceMetadataModule.runThroughDispatcher();
    try {
      registerInstanceMetadataModule.getResult(registrationTimeout_);
    } catch (NebuloException exception) {
      logger_.error("Unable to register InstanceMetadata!", exception);
    }
    registerInstanceKeys();
    registerUser(appKey);
    registerUserKeys();
  }

  private void registerUser(AppKey appKey) {
    // TODO(bolek): This should be part of broker. (szm): or NetworkMonitor

    ContractList contractList = new ContractList();
    try {
      contractList.addGroup(new ReplicationGroup(new CommAddress[] {commAddress_ },
        BigInteger.ZERO, new BigInteger("1000000")));
    } catch (IntervalCollisionException e) {
      logger_.error("Error while creating replication group", e);
    }
    PutKeyModule putKeyModule = new PutKeyModule(
        dispatcherInQueue_,
        new KeyDHT(appKey.getKey()),
        new ValueDHT(new UserMetadata(appKey, contractList)));
    try {
      putKeyModule.getResult(registrationTimeout_);
    } catch (NebuloException exception) {
      logger_.error("Unable to execute PutKeyModule!", exception);
    }
  }

  /**
   * Initialize all optional modules and schedule them for execution by dispatcher.
   * Override this method to run modules selectively.
   */
  protected void initializeModules() {
    runNetworkMonitor();
    runBroker();
    runAsyncMessaging();
  }

  /**
   * Logic to be executed when the application is already running.
   * Override this method when operations on active application are necessary.
   */
  protected void runActively() {
    // TODO: Move register to separate module or at least make it non-blocking.
    register(appKey_);
  }

  /**
   * Actions performed on exit.
   * Override this method when special clean-up is required.
   */
  protected void cleanModules() {
    // Empty by default.
  }

  protected void registerInstanceKeys() {
    try {
      DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(commAddress_, dispatcherInQueue_);
      dhtKeyHandler.save(new LocalDiscKeyHandler(instancePublicKeyPath_,
          KeyType.PUBLIC).load());
      encryption_.load(instancePublicKeyId_, dhtKeyHandler);
      encryption_.load(instancePrivateKeyId_, new LocalDiscKeyHandler(instancePrivateKeyPath_,
          KeyType.PRIVATE));
    } catch (CryptoException e) {
      throw new RuntimeException("Unable to load public/private keys", e);
    }
  }

  protected void registerUserKeys() {
    try {
      DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(appKey_, dispatcherInQueue_);
      dhtKeyHandler.save(new LocalDiscKeyHandler(userPublicKeyPath_,
          KeyType.PUBLIC).load());
      encryption_.load(userPublicKeyId_, dhtKeyHandler);
      encryption_.load(userPrivateKeyId_, new LocalDiscKeyHandler(userPrivateKeyPath_,
          KeyType.PRIVATE));
    } catch (CryptoException e) {
      throw new RuntimeException("Unable to load public/private keys", e);
    }
  }

  protected void runNetworkMonitor() {
    networkMonitor_.runThroughDispatcher();
  }

  protected void runBroker() {
    broker_.runThroughDispatcher();
  }

  protected void runAsyncMessaging() {
    synchroUpdateSequencer_.runThroughDispatcher();
    msgReceivingCheckerThread_.start();
    asyncMessagingModule_.runThroughDispatcher();
  }

  protected void startCoreThreads() {
    networkThread_.start();
    dispatcherThread_.start();
    if (isRestEnabled_) {
      restThread_.start();
    }
  }

  protected void joinCoreThreads() {
    // Wait for threads to finish execution.
    try {
      msgReceivingCheckerThread_.join();
      networkThread_.join();
      dispatcherThread_.join();
      if (isRestEnabled_) {
        restThread_.join();
      }

    } catch (InterruptedException exception) {
      logger_.fatal("Interrupted");
      return;
    }
  }
}
