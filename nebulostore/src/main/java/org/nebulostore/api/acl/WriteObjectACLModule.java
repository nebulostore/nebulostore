package org.nebulostore.api.acl;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.EncryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author lukaszsiczek
 */
public class WriteObjectACLModule extends ReturningJobModule<NebuloObject> {

  private static final Logger LOGGER = Logger.getLogger(WriteObjectACLModule.class);

  private WriteObjectACLVisitor createObjectACLVisitor_ = new WriteObjectACLVisitor();
  private NebuloAddress address_;
  private EncryptionAPI encryptionAPI_;
  private String userPublicKeyId_;
  private String userPrivateKeyId_;
  private AppKey appKey_;
  private Injector injector_;
  private NetworkMonitor networkMonitor_;
  private Set<AppKey> accessList_;

  protected class WriteObjectACLVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      try {
        ACLModuleUtils.checkOwner(address_, appKey_);
      } catch (NebuloException e) {
        endWithError(e);
      }

      NebuloFile accessFile;
      NebuloObject dataFile;
      List<String> accessFileVersions = new LinkedList<String>();
      List<String> dataFileVersions = new LinkedList<String>();

      try {
        accessFile = ACLModuleUtils.getAccessFile(networkMonitor_, encryptionAPI_,
            injector_, address_);
        accessFileVersions = accessFile.getVersions();
        SecretKey secretKey = ACLModuleUtils.getSecretKeyFromAccessFile(encryptionAPI_, appKey_,
            userPrivateKeyId_, accessFile);
        NebuloAddress dataAddress = new NebuloAddress(address_.getAppKey(), accessFile.getNextId());
        dataFile = ACLModuleUtils.getDataFile(
            networkMonitor_, encryptionAPI_, injector_, secretKey, dataAddress);
        dataFileVersions = dataFile.getVersions();
        accessFile.delete();
        dataFile.delete();
      } catch (NebuloException e) {
        LOGGER.info(e);
      }

      accessFile = new NebuloFile(address_);
      accessFile.setVersions(accessFileVersions);
      injector_.injectMembers(accessFile);

      try {
        SecretKey secretKey = CryptoUtils.generateSecretKey();
        ACLAccessData aclAccessData = buildACLAccessData(secretKey);
        accessFile.setEncryptWrapper(new EncryptWrapper(encryptionAPI_, userPrivateKeyId_));
        accessFile.write(CryptoUtils.serializeObject(aclAccessData), 0);
        NebuloAddress dataAddress = new NebuloAddress(address_.getAppKey(), accessFile.getNextId());
        dataFile = new NebuloFile(dataAddress);
        dataFile.setVersions(dataFileVersions);
        injector_.injectMembers(dataFile);
        dataFile.setEncryptWrapper(new EncryptWrapper(encryptionAPI_, secretKey));
        endWithSuccess(dataFile);
      } catch (NebuloException e) {
        endWithError(e);
      }
    }

  }

  @Inject
  public void setDependencies(
      EncryptionAPI encryptionAPI,
      @Named("UserPublicKeyId") String userPublicKeyId,
      @Named("UserPrivateKeyId") String userPrivateKeyId,
      AppKey appKey,
      Injector injector,
      NetworkMonitor networkMonitor) {
    encryptionAPI_ = encryptionAPI;
    userPublicKeyId_ = userPublicKeyId;
    userPrivateKeyId_ = userPrivateKeyId;
    appKey_ = appKey;
    injector_ = injector;
    networkMonitor_ = networkMonitor;
  }

  public void createNewNebuloFile(NebuloAddress address, Set<AppKey> accessList) {
    address_ = checkNotNull(address);
    accessList_ = checkNotNull(accessList);
    runThroughDispatcher();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(createObjectACLVisitor_);
  }

  public NebuloObject awaitResult(int timeoutSec) throws NebuloException {
    return getResult(timeoutSec);
  }

  private ACLAccessData buildACLAccessData(SecretKey secretKey) throws CryptoException {
    ACLAccessData aclAccessData = new ACLAccessData();
    aclAccessData.add(address_.getAppKey(),
        encryptionAPI_.encrypt(secretKey, userPublicKeyId_));
    for (AppKey appKey : accessList_) {
      String publicKey = networkMonitor_.getUserPublicKeyId(appKey);
      aclAccessData.add(appKey, encryptionAPI_.encrypt(secretKey, publicKey));
    }
    return aclAccessData;
  }
}
