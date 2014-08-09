package org.nebulostore.systest.async.messages;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.messages.AsynchronousMessage;
import org.nebulostore.systest.async.CounterModuleMessageForwarder;

/**
 * Asynchronous message for IncrementMessage.
 *
 * @author Piotr Malicki
 *
 */
public class AsynchronousIncrementMessage extends AsynchronousMessage {

  private static final long serialVersionUID = 5759297012938038050L;

  @Override
  public JobModule getHandler() {
    return new CounterModuleMessageForwarder(this);
  }

}
