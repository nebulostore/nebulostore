package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.SessionNegotiatorModule;

/**
 * @author lukaszsiczek
 */
public class DHOneAToBMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = -1741082921627138834L;
  private int ttl_;

  public DHOneAToBMessage(CommAddress sourceAddress,
      CommAddress destAddress, String sessionId, String sourceJobId,
      int ttl, EncryptedObject data) {
    super(sourceAddress, destAddress, sessionId, sourceJobId, data);
    ttl_ = ttl;
  }

  @Override
  public JobModule getHandler() {
    return new SessionNegotiatorModule();
  }

  public int getTTL() {
    return ttl_;
  }
}
