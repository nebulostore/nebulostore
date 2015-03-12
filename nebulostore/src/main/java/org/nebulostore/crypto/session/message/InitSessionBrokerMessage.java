package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorBrokerModule;

/**
 * @author lukaszsiczek
 */
public class InitSessionBrokerMessage extends InitSessionMessage {

  private static final long serialVersionUID = 8179640618218979552L;

  public InitSessionBrokerMessage(CommAddress sourceAddress, CommAddress destAddress,
      String sourceJobId, EncryptedObject data) {
    super(sourceAddress, destAddress, sourceJobId, data);
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorBrokerModule();
  }

}
