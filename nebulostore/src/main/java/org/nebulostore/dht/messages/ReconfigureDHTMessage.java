package org.nebulostore.dht.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * @author Marcin Walas
 */
public class ReconfigureDHTMessage extends Message {
  private static final long serialVersionUID = -6393928079883663656L;
  private final String provider_;

  public ReconfigureDHTMessage(String jobId, String provider) {
    super(jobId);
    provider_ = provider;
  }

  public String getProvider() {
    return provider_;
  }

}
