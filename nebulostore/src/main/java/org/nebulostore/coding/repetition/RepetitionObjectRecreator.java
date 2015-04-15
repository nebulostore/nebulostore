package org.nebulostore.coding.repetition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.coding.ObjectRecreator;
import org.nebulostore.communication.naming.CommAddress;

/**
 * Simple object recreator used for repetition code (full object replication).
 *
 * @author Piotr Malicki
 *
 */
public class RepetitionObjectRecreator implements ObjectRecreator {

  private EncryptedObject object_;
  private final List<CommAddress> replicators_ = new ArrayList<>();

  @Override
  public boolean addNextFragment(EncryptedObject fragment, CommAddress replicator) {
    if (replicators_.contains(replicator)) {
      object_ = fragment;
    }
    return object_ != null;
  }

  @Override
  public EncryptedObject recreateObject() throws NebuloException {
    if (object_ == null) {
      throw new NebuloException("Not enough fragments to recreate the object.");
    }
    return object_;
  }

  @Override
  public int getNeededFragmentsNumber() {
    return object_ == null ? 1 : 0;
  }

  @Override
  public void setReplicators(Collection<CommAddress> replicators) {
    replicators_.addAll(replicators);
  }

  @Override
  public List<CommAddress> calcReplicatorsToAsk() {
    if (replicators_.isEmpty()) {
      return new ArrayList<CommAddress>();
    }
    return Lists.newArrayList(replicators_.get(0));
  }

  @Override
  public void removeReplicator(CommAddress replicator) {
    replicators_.remove(replicator);
  }

}
