package org.nebulostore.crypto.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = -1741082921627138834L;

  public InitSessionMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, EncryptedObject data) {
    super(jobId, sourceAddress, destAddress, data);
  }
}
