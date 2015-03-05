package org.nebulostore.broker;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;

/**
 * Broker error message.
 * @author szymonmatejczyk
 */
public class BrokerErrorMessage extends Message {
  private static final long serialVersionUID = -4999136038489126015L;

  NebuloException error_;

  public NebuloException getError() {
    return error_;
  }

  public BrokerErrorMessage(NebuloException error) {
    super();
    error_ = error;
  }

  public BrokerErrorMessage(String jobId, NebuloException error) {
    super(jobId);
    error_ = error;
  }
}
