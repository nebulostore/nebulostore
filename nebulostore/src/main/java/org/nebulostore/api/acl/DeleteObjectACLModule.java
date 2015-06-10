package org.nebulostore.api.acl;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author lukaszsiczek
 */
public class DeleteObjectACLModule extends ReturningJobModule<Void> {

  private DeleteObjectACLVisitor deleteObjectACLVisitor_ = new DeleteObjectACLVisitor();
  private NebuloAddress address_;
  private EncryptionAPI encryptionAPI_;
  private String privateKeyPeerId_;
  private Injector injector_;
  private NetworkMonitor networkMonitor_;
  private AppKey appKey_;

  @Inject
  public void setDependencies(
      EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      Injector injector,
      NetworkMonitor networkMonitor,
      AppKey appKey) {
    encryptionAPI_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
    injector_ = injector;
    networkMonitor_ = networkMonitor;
    appKey_ = appKey;
  }

  protected class DeleteObjectACLVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      try {
        ACLModuleUtils.checkOwner(address_, appKey_);
        NebuloFile accessFile = ACLModuleUtils.getAccessFile(networkMonitor_, encryptionAPI_,
            injector_, address_);
        SecretKey secretKey = ACLModuleUtils.getSecretKeyFromAccessFile(
            encryptionAPI_, appKey_, privateKeyPeerId_, accessFile);
        NebuloAddress dataAddress = new NebuloAddress(address_.getAppKey(), accessFile.getNextId());
        accessFile.delete();
        NebuloObject dataFile = ACLModuleUtils.getDataFile(
            networkMonitor_, encryptionAPI_, injector_, secretKey, dataAddress);
        dataFile.delete();
        endWithSuccess(null);
      } catch (NebuloException e) {
        endWithError(e);
      }
    }
  }

  public void deleteObject(NebuloAddress address) {
    address_ = checkNotNull(address);
    runThroughDispatcher();
  }

  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(deleteObjectACLVisitor_);
  }

}
