package org.nebulostore.identity;

import java.math.BigInteger;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.bouncycastle.crypto.RuntimeCryptoException;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.keys.DHTKeyHandler;
import org.nebulostore.crypto.keys.KeyIdPair;
import org.nebulostore.crypto.keys.LocalDiscKeyHandler;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * @author lukaszsiczek
 */
public class IdentityManager {
  private static Logger logger_ = Logger.getLogger(IdentityManager.class);
  private static final int TIMEOUT_SEC = 60;

  private Map<AppKey, KeyIdPair> users_ = new HashMap<AppKey, KeyIdPair>();
  private Map<AppKey, KeyIdPair> guests_ = new HashMap<AppKey, KeyIdPair>();
  private AppKey currentUser_;
  private BlockingQueue<Message> dispatcherQueue_;
  private int guestsLimit_;
  private CommAddress instanceAddress_;
  private String currentUserPublicKeyId_;
  private String currentUserPrivateKeyId_;
  private EncryptionAPI encryption_;
  private NetworkMonitor networkMonitor_;

  @Inject
  public void setModuleDependencies(
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      @Named("identity-manager.guests-limit") int guestsLimit,
      CommAddress instanceAddress,
      EncryptionAPI encryption,
      NetworkMonitor networkMonitor) {
    dispatcherQueue_ = dispatcherQueue;
    guestsLimit_ = guestsLimit;
    instanceAddress_ = instanceAddress;
    encryption_ = encryption;
    networkMonitor_ = networkMonitor;
  }

  private void putDefaultContractListToDHT(AppKey appKey) {
    ContractList contractList = new ContractList();
    try {
      contractList.addGroup(new ReplicationGroup(new CommAddress[] {instanceAddress_ },
        BigInteger.ZERO, new BigInteger("1000000")));
    } catch (IntervalCollisionException e) {
      logger_.error("Error while creating replication group", e);
    }
    putContractListIntoDHT(appKey, contractList);
  }

  public void updateContractListDHT(ContractList contractList) throws NebuloException {
    for (AppKey appKey : getRegisterUsers()) {
      putContractListIntoDHT(appKey, contractList);
    }
  }

  public void login(AppKey appKey, String privateKeyPath) {
    try {
      login(appKey, CryptoUtils.readPrivateKeyFromPath(privateKeyPath));
    } catch (CryptoException e) {
      logger_.error(e);
      throw new RuntimeCryptoException(e.getMessage());
    }
  }

  public void login(AppKey appKey, PrivateKey privateKey) {
    loginOperation(appKey, privateKey, users_);
    System.out.println("Login " + appKey);
  }

  public void loginGuest(AppKey appKey, PrivateKey privateKey) {
    loginOperation(appKey, privateKey, guests_);
    System.out.println("Login Guest " + appKey);
  }

  private void loginOperation(AppKey appKey, PrivateKey privateKey,
      Map<AppKey, KeyIdPair> map) {
    if (!map.containsKey(appKey)) {
      logger_.error("User " + appKey + " must be register before login");
      return;
    }
    currentUser_ = appKey;
    KeyIdPair keyIdPair = map.get(appKey);
    currentUserPublicKeyId_ = keyIdPair.getPublicKeyId();
    currentUserPrivateKeyId_ = keyIdPair.getPrivateKeyId();
    encryption_.load(currentUserPrivateKeyId_, new LocalDiscKeyHandler(privateKey));
  }

  public void logout() {
    currentUser_ = null;
    encryption_.remove(currentUserPrivateKeyId_);
    currentUserPublicKeyId_ = null;
    currentUserPrivateKeyId_ = null;
  }

  public AppKey getCurrentUserAppKey() {
    return currentUser_;
  }

  public String getCurrentUserPublicKeyId() {
    return currentUserPublicKeyId_;
  }

  public String getCurrentUserPrivateKeyId() {
    return currentUserPrivateKeyId_;
  }

  private void putContractListIntoDHT(AppKey appKey, ContractList contractList) {
    UserMetadata userMetadata = new UserMetadata(appKey, contractList);
    PutKeyModule putKeyModule = new PutKeyModule(
        dispatcherQueue_,
        new KeyDHT(appKey.getKey()),
        new ValueDHT(userMetadata));
    try {
      putKeyModule.getResult(TIMEOUT_SEC);
    } catch (NebuloException exception) {
      logger_.error("Unable to execute PutKeyModule!", exception);
    }
  }

  public AppKey registerUserIdentity(PublicKey publicKey) {
    AppKey appKey = new AppKey(CryptoUtils.getRandomNumber(new BigInteger("100")));
    String publicKeyId = CryptoUtils.getRandomString();
    String privateKeyId = CryptoUtils.getRandomString();
    try {
      DHTKeyHandler dhtKeyHandler = new DHTKeyHandler(appKey, dispatcherQueue_);
      dhtKeyHandler.save(new LocalDiscKeyHandler(publicKey).load());
      encryption_.load(publicKeyId, dhtKeyHandler);
    } catch (CryptoException e) {
      throw new RuntimeException("Unable to load public/private keys", e);
    }
    users_.put(appKey, new KeyIdPair(publicKeyId, privateKeyId));
    putDefaultContractListToDHT(appKey);
    return appKey;
  }

  public void registerGuestIdentity(AppKey appKey) {
    if (guests_.size() > guestsLimit_) {
      throw new RuntimeException("Unable to register guest. Guest limit has been exceeded");
    }
    String publicKeyId = networkMonitor_.getUserPublicKeyId(appKey);
    String privateKeyId = CryptoUtils.getRandomString();
    guests_.put(appKey, new KeyIdPair(publicKeyId, privateKeyId));
  }

  public void unregisterGuestIdentity(AppKey appKey) {
    guests_.remove(appKey);
  }

  public AppKey registerUserIdentity(String publicKeyPath) {
    try {
      return registerUserIdentity(CryptoUtils.readPublicKeyFromPath(publicKeyPath));
    } catch (CryptoException e) {
      throw new RuntimeException("Unable to register user", e);
    }
  }

  public Collection<AppKey> getRegisterUsers() {
    return users_.keySet();
  }

  public Collection<AppKey> getRegusterGuests() {
    return guests_.keySet();
  }
}
