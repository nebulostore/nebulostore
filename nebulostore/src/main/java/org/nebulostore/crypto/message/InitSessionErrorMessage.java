package org.nebulostore.crypto.message;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionErrorMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = 6523437594110697492L;

  public InitSessionErrorMessage(String jobId, CommAddress sourceAddress, CommAddress destAddress) {
    super(jobId, sourceAddress, destAddress, null);
  }

}
