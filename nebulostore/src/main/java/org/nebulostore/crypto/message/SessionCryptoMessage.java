package org.nebulostore.crypto.message;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.broker.BrokerMessageForwarder;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public abstract class SessionCryptoMessage extends CommMessage {

  private static final long serialVersionUID = -920124952136609208L;
  private EncryptedObject data_;

  public SessionCryptoMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, EncryptedObject data) {
    super(jobId, sourceAddress, destAddress);
    data_ = data;
  }

  public EncryptedObject getEncryptedData() {
    return data_;
  }

  @Override
  public JobModule getHandler() throws NebuloException {
    return new BrokerMessageForwarder(this);
  }
}
