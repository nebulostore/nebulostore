package org.nebulostore.appcore;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.Mergeable;
import org.nebulostore.networkmonitor.PeerConnectionSurvey;

/**
 * Metadata stored in DHT for Nebulostore instance.
 *
 * @author szymonmatejczyk
 */
public class InstanceMetadata implements Serializable, Mergeable {
  private static final long serialVersionUID = -2246471507395388278L;

  /* Id of user, that this metadata applies to. */
  private final AppKey owner_;

  /* Communication addresses of peers that store messages for @instance. */
  private final Set<CommAddress> synchroGroup_ = new HashSet<>();

  /* Communication addresses of peers for which @instance store messages. */
  private final Set<CommAddress> recipients_ = new HashSet<>();

  private final ConcurrentLinkedQueue<PeerConnectionSurvey> statistics_ =
      new ConcurrentLinkedQueue<PeerConnectionSurvey>();

  public InstanceMetadata(AppKey owner) {
    owner_ = owner;
  }

  public Set<CommAddress> getSynchroGroup() {
    return synchroGroup_;
  }

  public Set<CommAddress> getRecipients() {
    return recipients_;
  }

  @Override
  public Mergeable merge(Mergeable other) {
    // TODO(SZM): remove duplicated old statistics - design issue
    if (other instanceof InstanceMetadata) {
      InstanceMetadata o = (InstanceMetadata) other;
      synchroGroup_.addAll(o.synchroGroup_);
      recipients_.addAll(o.recipients_);
      // TODO
    }
    return this;
  }

  public ConcurrentLinkedQueue<PeerConnectionSurvey> getStatistics() {
    return statistics_;
  }

  @Override
  public String toString() {
    return "InstanceMetadata: owner: " + owner_.toString() + " synchro-peers num: " +
        synchroGroup_.size();
  }
}
