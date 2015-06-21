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
public class ReadObjectACLModule extends ReturningJobModule<NebuloObject> {

  private ReadObjectACLVisitor readObjectACLVisitor_ = new ReadObjectACLVisitor();
  private NebuloAddress address_;
  private EncryptionAPI encryptionAPI_;
  private String instancePrivateKeyId_;
  private Injector injector_;
  private NetworkMonitor networkMonitor_;
  private AppKey appKey_;

  @Inject
  public void setDependencies(
      EncryptionAPI encryptionAPI,
      @Named("InstancePrivateKeyId") String instancePrivateKeyId,
      Injector injector,
      NetworkMonitor networkMonitor,
      AppKey appKey) {
    encryptionAPI_ = encryptionAPI;
    instancePrivateKeyId_ = instancePrivateKeyId;
    injector_ = injector;
    networkMonitor_ = networkMonitor;
    appKey_ = appKey;
  }

  public void fetchObject(NebuloAddress address) {
    address_ = checkNotNull(address);
    runThroughDispatcher();
  }

  protected class ReadObjectACLVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      try {
        NebuloFile accessFile = ACLModuleUtils.getAccessFile(networkMonitor_, encryptionAPI_,
            injector_, address_);
        SecretKey secretKey = ACLModuleUtils.getSecretKeyFromAccessFile(encryptionAPI_, appKey_,
            instancePrivateKeyId_, accessFile);
        NebuloAddress dataAddress = new NebuloAddress(address_.getAppKey(), accessFile.getNextId());
        NebuloObject dataFile = ACLModuleUtils.getDataFile(
            networkMonitor_, encryptionAPI_, injector_, secretKey, dataAddress);
        endWithSuccess(dataFile);
      } catch (NebuloException error) {
        endWithError(error);
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(readObjectACLVisitor_);
  }

  public NebuloObject awaitResult(int timeoutSec) throws NebuloException {
    return getResult(timeoutSec);
  }

}
