package org.nebulostore.broker;

import java.math.BigInteger;

import com.google.inject.Inject;

import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
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

  @Inject
  private void setDependencies(CommAddress myAddress,
                               NetworkMonitor networkMonitor,
                               BrokerContext context,
                               AppKey appKey) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    context_ = context;
    appKey_ = appKey;
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
        new ValueDHT(new Metadata(appKey_, contractList)));
    putKeyModule.getResult(timeoutSec);
  }
}
