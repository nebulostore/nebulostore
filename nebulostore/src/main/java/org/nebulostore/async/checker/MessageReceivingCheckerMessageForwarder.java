package org.nebulostore.async.checker;

import com.google.inject.Inject;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

/**
 * Message forwarder for message receiving checker module.
 *
 * @author Piotr Malicki
 *
 */
public class MessageReceivingCheckerMessageForwarder extends JobModule {

  private MessageReceivingCheckerModule module_;
  private final Message message_;

  public MessageReceivingCheckerMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setDependencies(MessageReceivingCheckerModule module) {
    module_ = module;
  }

  @Override
  protected void initModule() {
    super.initModule();
    module_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  public boolean isQuickNonBlockingTask() {
    return true;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
  }
}
