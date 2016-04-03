package org.nebulostore.appcore.model;

import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Injector;

import org.nebulostore.api.acl.DeleteObjectACLModule;
import org.nebulostore.api.acl.ReadObjectACLModule;
import org.nebulostore.api.acl.WriteObjectACLModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.crypto.DecryptWrapper;
import org.nebulostore.crypto.EncryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.identity.IdentityManager;

/**
 * Factory that produces objects for NebuloStore users.
 * @author Bolek Kulbabinski
 */
public class NebuloObjectFactoryImpl implements NebuloObjectFactory {

  private static final int TIMEOUT_SEC = 60;

  // Needed to inject dependencies to objects fetched from the network.
  protected Injector injector_;
  private EncryptionAPI encryptionAPI_;
  private String userPublicKeyId_;
  private String userPrivateKeyId_;

  @Inject
  public void setDependencies(Injector injector,
      EncryptionAPI encryptionAPI,
      IdentityManager identityManager) {
    injector_ = injector;
    encryptionAPI_ = encryptionAPI;
    userPublicKeyId_ = identityManager.getCurrentUserPublicKeyId();
    userPrivateKeyId_ = identityManager.getCurrentUserPrivateKeyId();
  }

  @Override
  public NebuloObject fetchExistingNebuloObject(NebuloAddress address) throws NebuloException {
    DecryptWrapper decryptWrapper = new DecryptWrapper(encryptionAPI_, userPrivateKeyId_);
    ObjectGetter getter = injector_.getInstance(ObjectGetter.class);
    getter.fetchObject(address, decryptWrapper);
    NebuloObject result = getter.awaitResult(TIMEOUT_SEC);
    result.setDecryptWrapper(decryptWrapper);
    injector_.injectMembers(result);
    return result;
  }

  @Override
  public NebuloObject fetchNebuloObject(NebuloAddress address) throws NebuloException {
    ReadObjectACLModule getter = injector_.getInstance(ReadObjectACLModule.class);
    getter.fetchObject(address);
    return getter.awaitResult(TIMEOUT_SEC);
  }

  @Override
  public NebuloFile createNewNebuloFile(NebuloAddress address) {
    NebuloFile file = new NebuloFile(address);
    file.setEncryptWrapper(new EncryptWrapper(encryptionAPI_, userPublicKeyId_));
    injector_.injectMembers(file);
    return file;
  }

  @Override
  public NebuloFile createNewAccessNebuloFile(NebuloAddress address, Set<AppKey> accessList)
      throws NebuloException {
    WriteObjectACLModule createObjectACLModule = injector_.getInstance(WriteObjectACLModule.class);
    createObjectACLModule.createNewNebuloFile(address, accessList);
    return (NebuloFile) createObjectACLModule.awaitResult(TIMEOUT_SEC);
  }

  @Override
  public NebuloList createNewNebuloList(NebuloAddress address) {
    NebuloList list = new NebuloList(address);
    list.setEncryptWrapper(new EncryptWrapper(encryptionAPI_, userPublicKeyId_));
    injector_.injectMembers(list);
    return list;
  }

  @Override
  public void deleteNebuloObject(NebuloAddress address) throws NebuloException {
    DeleteObjectACLModule deleteObjectACLModule =
        injector_.getInstance(DeleteObjectACLModule.class);
    deleteObjectACLModule.deleteObject(address);
    deleteObjectACLModule.awaitResult(TIMEOUT_SEC);
  }
}
