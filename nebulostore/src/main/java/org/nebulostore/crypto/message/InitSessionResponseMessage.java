package org.nebulostore.crypto.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionResponseMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = -5774260936905244967L;

  public InitSessionResponseMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, EncryptedObject data) {
    super(jobId, sourceAddress, destAddress, data);
  }

}
