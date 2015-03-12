package org.nebulostore.crypto.session.message;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class InitSessionErrorMessage extends SessionCryptoMessage {

  private static final long serialVersionUID = 6523437594110697492L;

  public String errorMessage_;

  public InitSessionErrorMessage(String jobId, CommAddress sourceAddress,
      CommAddress destAddress, String errorMessage) {
    super(jobId, sourceAddress, destAddress, null, null);
    errorMessage_ = errorMessage;
  }

  public String getErrorMessage() {
    return errorMessage_;
  }
}
