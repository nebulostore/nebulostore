package org.nebulostore.replicator.core;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.nebulostore.appcore.modules.JobModule;

/**
 * Base class for all replicators.
 *
 * @author Bolek Kulbabinski
 */
public abstract class Replicator extends JobModule {

  public class MetaData {
    private String objectId_;
    private Integer size_;

    public MetaData(String objectId, Integer size) {
      this.objectId_ = objectId;
      this.size_ = size;
    }

    public String getObjectId() {
      return objectId_;
    }

    public Integer getSize() {
      return size_;
    }
  }

  protected Map<String, MetaData> storedObjectsMeta_;

  public Replicator(Map<String, MetaData> storedObjectsIds) {
    storedObjectsMeta_ = storedObjectsIds;
  }

  public Set<String> getStoredObjectsIds() {
    return Collections.unmodifiableSet(storedObjectsMeta_.keySet());
  }

  public Map<String, MetaData> getStoredMetaData() {
    return Collections.unmodifiableMap(storedObjectsMeta_);
  }
}
