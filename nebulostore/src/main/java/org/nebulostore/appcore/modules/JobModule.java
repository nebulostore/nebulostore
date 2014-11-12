package org.nebulostore.appcore.modules;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dispatcher.JobEndedMessage;
import org.nebulostore.dispatcher.JobInitMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for all job handlers - modules that are managed by dispatcher.
 * Message queues outQueue_ and inQueue_ are always connected to dispatcher.
 * @author Bolek Kulbabinski
 */
public abstract class JobModule extends Module {
  private static Logger logger_ = Logger.getLogger(JobModule.class);

  protected BlockingQueue<Message> networkQueue_;
  protected String jobId_;
  private boolean isStarted_;

  public JobModule() {
    jobId_ = CryptoUtils.getRandomString();
  }

  public JobModule(String jobId) {
    jobId_ = jobId;
  }

  @Inject
  public void setNetworkQueue(@Named("NetworkQueue") BlockingQueue<Message> networkQueue) {
    networkQueue_ = networkQueue;
  }

  @Inject
  public void setDispatcherQueue(@Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue) {
    outQueue_ = dispatcherQueue;
  }

  public void setJobId(String jobId) {
    jobId_ = jobId;
  }

  public String getJobId() {
    return jobId_;
  }

  /**
   * Run this module through a JobInitMessage (with new random ID) sent to Dispatcher.
   */
  public synchronized void runThroughDispatcher() {
    logger_.debug("Running module of type: " + getClass().getName() + " through the dispatcher.");
    checkNotNull(jobId_);
    if (isStarted_) {
      logger_.error("Module already ran.");
      return;
    }

    isStarted_ = true;
    logger_.debug("Adding JobInitMessage to the outQueue_");
    outQueue_.add(new JobInitMessage(this));
  }

  /**
   * Returning true will result in running this JobModule in Dispatcher's thread (without creating
   * new one). Use with care as it may easily slow down or block Dispatcher and the whole system.
   * When this is set to true, module should perform a fast operation and exit immediately.
   */
  public boolean isQuickNonBlockingTask() {
    return false;
  }

  /**
   * IMPORTANT: This should be the very LAST method to call for a dying thread.
   */
  protected final void endJobModule() {
    // Inform run() (in base class) that this thread is ready to die.
    endModule();

    // Inform dispatcher that we are going to die.
    if (!isQuickNonBlockingTask()) {
      outQueue_.add(new JobEndedMessage(checkNotNull(jobId_)));
    }
  }
}
