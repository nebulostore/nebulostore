package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public abstract class InitSessionMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = -1741082921627138834L;

  public InitSessionMessage(CommAddress sourceAddress,
      CommAddress destAddress, String sourceJobId, EncryptedObject data) {
    super(sourceAddress, destAddress, sourceJobId, data);
  }

  @Override
  public abstract JobModule getHandler();
}
