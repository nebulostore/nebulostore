package org.nebulostore.broker;

import java.math.BigInteger;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.networkmonitor.NetworkMonitor;


/**
 * Broker is always a singleton job. See BrokerMessageForwarder.
 * @author Bolek Kulbabinski
 */
public abstract class Broker extends JobModule {
  protected CommAddress myAddress_;

  protected NetworkMonitor networkMonitor_;
  protected BrokerContext context_;
  private AppKey appKey_;
  protected EncryptionAPI encryptionAPI_;
  protected String instancePrivateKeyId_;

  @Inject
  private void setDependencies(CommAddress myAddress,
                               NetworkMonitor networkMonitor,
                               BrokerContext context,
                               AppKey appKey,
                               EncryptionAPI encryptionAPI,
                               @Named("InstancePrivateKeyId") String instancePrivateKeyId) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    context_ = context;
    appKey_ = appKey;
    instancePrivateKeyId_ = instancePrivateKeyId;
    encryptionAPI_ = encryptionAPI;
  }

  public void updateReplicationGroups(int timeoutSec) throws NebuloException {
    // todo(szm): one group for now
    ContractList contractList = new ContractList();
    try {
      contractList.addGroup(new ReplicationGroup(
          context_.getReplicas(), BigInteger.ZERO, new BigInteger("1000000")));
    } catch (IntervalCollisionException e) {
      throw new NebuloException("Error while creating replication group", e);
    }
    PutKeyModule putKeyModule = new PutKeyModule(
        outQueue_,
        new KeyDHT(appKey_.getKey()),
        new ValueDHT(new UserMetadata(appKey_, contractList)));
    putKeyModule.getResult(timeoutSec);
  }
}
