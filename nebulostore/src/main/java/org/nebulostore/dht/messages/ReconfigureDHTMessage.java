package org.nebulostore.dht.messages;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;

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

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
