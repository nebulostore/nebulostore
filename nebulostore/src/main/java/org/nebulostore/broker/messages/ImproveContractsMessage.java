package org.nebulostore.broker.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message send by an instance to itself to start ImproveContractsModule.
 *
 * @author szymonmatejczyk
 *
 */
public class ImproveContractsMessage extends Message {
  private static final long serialVersionUID = 3986327905857030018L;

  public ImproveContractsMessage(String jobId) {
    super(jobId);
  }

}
