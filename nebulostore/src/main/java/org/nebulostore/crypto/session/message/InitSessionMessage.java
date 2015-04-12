package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorModule;

/**
 * @author lukaszsiczek
 */
public class InitSessionMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = -1741082921627138834L;

  public InitSessionMessage(CommAddress sourceAddress,
      CommAddress destAddress, String sessionId, String sourceJobId, EncryptedObject data) {
    super(sourceAddress, destAddress, sessionId, sourceJobId, data);
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorModule();
  }
}
