package org.nebulostore.dispatcher;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

/**
 * This is a generic message to start a job via dispatcher.
 */
public class JobInitMessage extends Message {
  private static final long serialVersionUID = 2482967055598180345L;
  private final transient JobModule jobModule_;

  public JobInitMessage(JobModule jobModule) {
    super(jobModule.getJobId());
    jobModule_ = jobModule;
  }

  public JobInitMessage(String jobId, JobModule jobModule) {
    super(jobId);
    jobModule_ = jobModule;
  }

  @Override
  public JobModule getHandler() {
    return jobModule_;
  }
}
