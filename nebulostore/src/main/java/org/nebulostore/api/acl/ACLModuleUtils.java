package org.nebulostore.api.acl;

import javax.crypto.SecretKey;

import com.google.inject.Injector;

import org.bouncycastle.util.Arrays;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObject;
import org.nebulostore.appcore.model.ObjectGetter;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.DecryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * @author lukaszsiczek
 */
public final class ACLModuleUtils {

  private static final int TIMEOUT_SEC = 60;

  private ACLModuleUtils() {
  }

  public static NebuloFile getAccessFile(
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      Injector injector,
      NebuloAddress address) throws NebuloException {
    String publicKey = networkMonitor.getPeerPublicKeyId(
        ACLModuleUtils.tmpAppKeyToCommAddress(address.getAppKey()));
    DecryptWrapper accessDecryptWrapper = new DecryptWrapper(encryptionAPI, publicKey);

    ObjectGetter getter = injector.getInstance(ObjectGetter.class);
    getter.fetchObject(address, accessDecryptWrapper);
    NebuloFile accessFile = (NebuloFile) getter.awaitResult(TIMEOUT_SEC);
    accessFile.setDecryptWrapper(accessDecryptWrapper);
    injector.injectMembers(accessFile);
    return accessFile;
  }

  public static NebuloObject getDataFile(
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      Injector injector,
      SecretKey secretKey,
      NebuloAddress address) throws NebuloException {
    ObjectGetter getter = injector.getInstance(ObjectGetter.class);
    DecryptWrapper dataDecryptWrapper = new DecryptWrapper(encryptionAPI, secretKey);
    getter.fetchObject(address, dataDecryptWrapper);
    NebuloObject nebuloObject = getter.awaitResult(TIMEOUT_SEC);
    nebuloObject.setDecryptWrapper(dataDecryptWrapper);
    injector.injectMembers(nebuloObject);
    return nebuloObject;
  }

  public static SecretKey getSecretKeyFromAccessFile(
      EncryptionAPI encryptionAPI,
      AppKey appKey,
      String privateKeyPeerId,
      NebuloFile accessFile) throws NebuloException {
    byte[] rowData = new byte[0];
    int pos = 0;
    int len = 100;
    byte[] data;
    do {
      data = accessFile.read(pos, len);
      pos += data.length;
      rowData = Arrays.concatenate(rowData, data);
    } while (data.length > 0);
    ACLAccessData accessData = (ACLAccessData) CryptoUtils.deserializeObject(rowData);
    return (SecretKey) encryptionAPI.decrypt(accessData.get(appKey), privateKeyPeerId);
  }

  public static void checkOwner(NebuloAddress address, AppKey appKey) throws NebuloException {
    if (!appKey.equals(address.getAppKey())) {
      throw new NebuloException("Owner AppKey must be equal to NebuloAddress's AppKey");
    }
  }

  public static CommAddress tmpAppKeyToCommAddress(AppKey appKey) {
    int i = 11;
    int id = 1;
    while (i < appKey.getKey().intValue()) {
      i += 11;
      ++id;
    }
    return new CommAddress("00000000-0000-0000-" + String.format("%04d", id) +  "-000000000000");
  }
}
