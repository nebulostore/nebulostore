package org.nebulostore.systest.async;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

public class CounterModuleMessageForwarder extends JobModule {

  private static Logger logger_ = Logger.getLogger(CounterModuleMessageForwarder.class);

  CounterModule counterModule_;
  Message message_;

  public CounterModuleMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setDependencies(CounterModule counterModule) {
    counterModule_ = counterModule;
  }

  @Override
  protected void initModule() {
    super.initModule();
    counterModule_.getInQueue().add(message_);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    logger_.info("Ignoring message in " + getClass().getCanonicalName() + ".");
  }

}
