package org.nebulostore.crypto.messages;

import java.security.PublicKey;

import org.nebulostore.appcore.messaging.Message;

/**
 * @author lukaszsiczek
 */
public class PublicKeyMessage extends Message {

  private static final long serialVersionUID = 1327924836210675596L;
  private final PublicKey publicKey_;

  public PublicKeyMessage(String jobId, PublicKey publicKey) {
    super(jobId);
    publicKey_ = publicKey;
  }

  public PublicKey getPublicKey() {
    return publicKey_;
  }
}
