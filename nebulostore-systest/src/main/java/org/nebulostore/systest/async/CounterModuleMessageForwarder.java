package org.nebulostore.systest.async;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;

/**
 * JobModule responsible for forwarding messages to the counter module.
 *
 * @author Piotr Malicki
 *
 */
public class CounterModuleMessageForwarder extends JobModule {

  private static Logger logger_ = Logger.getLogger(CounterModuleMessageForwarder.class);

  private CounterModule counterModule_;
  private final Message message_;

  public CounterModuleMessageForwarder(Message message) {
    message_ = message;
  }

  @Inject
  public void setDependencies(CounterModule counterModule) {
    logger_.debug("setting counter module: " + counterModule);
    counterModule_ = counterModule;
  }

  @Override
  public void initModule() {
    super.initModule();
    counterModule_.getInQueue().add(message_);
    endJobModule();
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    logger_.info("Ignoring message in " + getClass().getCanonicalName() + ".");
  }

}
