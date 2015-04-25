package org.nebulostore.replicator.repairer;

import java.util.Collection;
import java.util.List;

import com.google.inject.assistedinject.Assisted;

import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.broker.ReplicatorData;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author Piotr Malicki
 */
public interface ReplicaRepairerModuleFactory {

  ReplicaRepairerModule createRepairer(
      @Assisted("CurrentReplicators") List<CommAddress> availableReplicators,
      @Assisted("NewReplicators") List<ReplicatorData> newReplicators,
      Collection<ObjectId> objectIds);

}
