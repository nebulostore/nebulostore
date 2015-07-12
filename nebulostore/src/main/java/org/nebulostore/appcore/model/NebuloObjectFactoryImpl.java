package org.nebulostore.appcore.model;

import java.math.BigInteger;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import org.nebulostore.api.acl.DeleteObjectACLModule;
import org.nebulostore.api.acl.ReadObjectACLModule;
import org.nebulostore.api.acl.WriteObjectACLModule;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.DecryptWrapper;
import org.nebulostore.crypto.EncryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;

/**
 * Factory that produces objects for NebuloStore users.
 * @author Bolek Kulbabinski
 */
public class NebuloObjectFactoryImpl implements NebuloObjectFactory {
  /**
   * For now default contract in nebulostore is set for objects of ids up to
   * 1000000. In order to make nebulostore work out of the box we limit the
   * random id of object ids to that number as well.  TODO. Please change it
   * once we have contract negotation working properly.
   */
  private static final BigInteger DEFAULT_OBJECTID_LIMIT =
      new BigInteger("1000000");
  private static final int TIMEOUT_SEC = 60;

  // Needed to inject dependencies to objects fetched from the network.
  protected Injector injector_;
  private EncryptionAPI encryptionAPI_;
  private String userPublicKeyId_;
  private String userPrivateKeyId_;

  @Inject
  public void setDependencies(Injector injector, EncryptionAPI encryptionAPI,
      @Named("UserPublicKeyId") String userPublicKeyId,
      @Named("UserPrivateKeyId") String userPrivateKeyId) {
    injector_ = injector;
    encryptionAPI_ = encryptionAPI;
    userPublicKeyId_ = userPublicKeyId;
    userPrivateKeyId_ = userPrivateKeyId;
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
  public NebuloFile createNewNebuloFile() {
    // TODO(bolek): Here should come more sophisticated ID generation method to account for
    //   (probably) fixed replication groups with ID intervals. (ask Broker? what size?)
    return createNewNebuloFile(new ObjectId(CryptoUtils.getRandomId().mod(DEFAULT_OBJECTID_LIMIT)));
  }

  @Override
  public NebuloFile createNewNebuloFile(ObjectId objectId) {
    NebuloAddress address = new NebuloAddress(injector_.getInstance(AppKey.class), objectId);
    return createNewNebuloFile(address);
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
  public NebuloList createNewNebuloList() {
    ObjectId objectId = new ObjectId(CryptoUtils.getRandomId().mod(DEFAULT_OBJECTID_LIMIT));
    return createNewNebuloList(objectId);
  }

  @Override
  public NebuloList createNewNebuloList(ObjectId objectId) {
    NebuloAddress address = new NebuloAddress(injector_.getInstance(AppKey.class), objectId);
    return createNewNebuloList(address);
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
