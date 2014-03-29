package org.nebulostore.replicator.messages;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.replicator.core.Replicator;

/**
 * Messages handled by current replicator implementation.
 *
 * @author Bolek Kulbabinski
 */
public abstract class InReplicatorMessage extends ReplicatorMessage {
  private static final long serialVersionUID = -4470381477373456845L;
  private Provider<Replicator> replicatorProvider_;

  public InReplicatorMessage(CommAddress destAddress) {
    super(destAddress);
  }

  public InReplicatorMessage(String jobId, CommAddress destAddress) {
    super(jobId, destAddress);
  }

  @Inject
  public void setReplicatorProvider(Provider<Replicator> replicatorProvider) {
    replicatorProvider_ = replicatorProvider;
  }

  @Override
  public JobModule getHandler() {
    return replicatorProvider_.get();
  }
}
