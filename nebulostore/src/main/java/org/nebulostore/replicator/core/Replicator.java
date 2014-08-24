package org.nebulostore.replicator.core;

import java.util.Collections;
import java.util.Set;

import org.nebulostore.appcore.modules.JobModule;

/**
 * Base class for all replicators.
 *
 * @author Bolek Kulbabinski
 */
public abstract class Replicator extends JobModule {
  protected Set<String> storedObjectsIds_;

  public Replicator(Set<String> storedObjectsIds) {
    storedObjectsIds_ = storedObjectsIds;
  }

  public Set<String> getStoredObjectsIds() {
    return Collections.unmodifiableSet(storedObjectsIds_);
  }
}
