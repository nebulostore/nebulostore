package org.nebulostore.appcore;

import java.io.Serializable;
import java.security.Key;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.Mergeable;
import org.nebulostore.networkmonitor.StatisticsList;

/**
 * Metadata stored in DHT for Nebulostore instance.
 *
 * @author szymonmatejczyk
 */
public class InstanceMetadata implements Serializable, Mergeable {
  private static final long serialVersionUID = -2246471507395388278L;

  private static final double STATISTICS_LIST_MEAN_WEIGHT_SINGLE = 0.7;
  private static final double STATISTICS_LIST_MEAN_WEIGHT_MULTI = 0.4;
  private static final int STATISTICS_LIST_MAX_SIZE = 1000;

  /* Id of user, that this metadata applies to. */
  private AppKey owner_;

  /* Communication addresses of peers that store messages for @instance. */
  private Set<CommAddress> synchroGroup_;

  /* Communication addresses of peers for which @instance stores messages. */
  private Set<CommAddress> recipients_;
  private int recipientsSetVersion_;

  private Key peerKey_;

  /**
   * Map with counters indicating number of times each peer was added as a synchro peer of this
   * instance.
   */
  private Map<CommAddress, Integer> synchroPeerCounters_ = new HashMap<>();

  private final StatisticsList statistics_ = new StatisticsList(STATISTICS_LIST_MEAN_WEIGHT_SINGLE,
      STATISTICS_LIST_MEAN_WEIGHT_MULTI, STATISTICS_LIST_MAX_SIZE);

  public InstanceMetadata() {
  }

  public InstanceMetadata(AppKey owner) {
    owner_ = owner;
  }

  public InstanceMetadata(AppKey owner, Set<CommAddress> synchroGroup, Set<CommAddress> recipients,
      Map<CommAddress, Integer> synchroPeerCounters) {
    owner_ = owner;
    synchroGroup_ = synchroGroup;
    recipients_ = recipients;
    synchroPeerCounters_ = synchroPeerCounters;
  }

  public Set<CommAddress> getSynchroGroup() {
    return synchroGroup_;
  }

  public void setSynchroGroup(Set<CommAddress> synchroGroup) {
    synchroGroup_ = synchroGroup;
  }

  public Set<CommAddress> getRecipients() {
    return recipients_;
  }

  public void setRecipients(Set<CommAddress> recipients) {
    recipients_ = recipients;
  }

  public int getRecipientsSetVersion() {
    return recipientsSetVersion_;
  }

  public void setRecipientsSetVersion(int recipientsSetVersion) {
    recipientsSetVersion_ = recipientsSetVersion;
  }

  public AppKey getOwner() {
    return owner_;
  }

  public Map<CommAddress, Integer> getSynchroPeerCounters() {
    return synchroPeerCounters_;
  }

  public void setSynchroPeerCounters(Map<CommAddress, Integer> recipientsCounters) {
    synchroPeerCounters_ = recipientsCounters;
  }

  public Key getPeerKey() {
    return peerKey_;
  }

  public void setPeerKey(Key peerKey) {
    peerKey_ = peerKey;
  }

  @Override
  public Mergeable merge(Mergeable other) {
    // TODO(SZM): remove duplicated old statistics - design issue
    if (other instanceof InstanceMetadata) {
      InstanceMetadata o = (InstanceMetadata) other;

      if (owner_ == null) {
        owner_ = o.owner_;
      }

      statistics_.addAllInFront(o.statistics_);

      if (synchroGroup_ == null) {
        synchroGroup_ = o.synchroGroup_;
      }

      if (synchroPeerCounters_ == null) {
        synchroPeerCounters_ = o.synchroPeerCounters_;
      } else if (o.synchroPeerCounters_ != null) {
        for (Entry<CommAddress, Integer> entry : o.synchroPeerCounters_.entrySet()) {
          int counter = entry.getValue();
          if (synchroPeerCounters_.containsKey(entry.getKey())) {
            counter = Math.max(counter, synchroPeerCounters_.get(entry.getKey()));
          }
          synchroPeerCounters_.put(entry.getKey(), counter);
        }
      }

      if (recipients_ == null || recipientsSetVersion_ <= o.recipientsSetVersion_) {
        recipients_ = o.recipients_;
        recipientsSetVersion_ = o.recipientsSetVersion_;
      }

      if (peerKey_ == null) {
        peerKey_ = o.peerKey_;
      }
    }
    return this;
  }

  public StatisticsList getStatistics() {
    return statistics_;
  }

  @Override
  public String toString() {
    return "InstanceMetadata: owner: " + owner_ + "\n\t" + "SynchroGroup: " +
        synchroGroup_ + "\n\t" + "Recipients: " + recipients_ + "\n\t" +
        "recipients set version: " + recipientsSetVersion_ + "\n\t" +
        "peer key: " + peerKey_ + "\n\t" +
        " statistics list size: " + statistics_.getAllStatisticsView().size() + "\n\t";
  }
}
