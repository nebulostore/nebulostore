package org.nebulostore.networkmonitor.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.networkmonitor.NetworkMonitorForwarder;

/**
 * Message send to test connection.
 *
 * If handler_ is set it is handled by it. If not it is send to NetworkMonitor via
 * NetworkMonitorForwarder.
 */
public class ConnectionTestMessage extends CommMessage {
  private static final long serialVersionUID = 4478191855169810054L;

  public ConnectionTestMessage(String jobId, CommAddress destAddress) {
    super(jobId, null, destAddress);
  }

  private JobModule handler_;

  @Override
  public JobModule getHandler() throws NebuloException {
    if (handler_ == null) {
      return new NetworkMonitorForwarder(this);
    } else {
      return handler_;
    }
  }

  public void setHandler(JobModule handler) {
    handler_ = handler;
  }
}
