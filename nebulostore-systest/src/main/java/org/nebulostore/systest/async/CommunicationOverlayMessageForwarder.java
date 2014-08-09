package org.nebulostore.systest.async;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

public class CommunicationOverlayMessageForwarder extends JobModule {

  private AsyncTestCommunicationOverlay commOverlay_;
  private final Message message_;

  public CommunicationOverlayMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setDependencies(AsyncTestCommunicationOverlay commOverlay) {
    commOverlay_ = commOverlay;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  public void initModule() {
    commOverlay_.getInQueue().add(message_);
    endJobModule();
  }
}
