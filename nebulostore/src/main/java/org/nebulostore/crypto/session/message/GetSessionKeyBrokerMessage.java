package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorBrokerModule;

/**
 * @author lukaszsiczek
 */
public class GetSessionKeyBrokerMessage extends GetSessionKeyMessage {

  private static final long serialVersionUID = 6264016849610986350L;

  public GetSessionKeyBrokerMessage(CommAddress peerAddress, String sourceJobId) {
    super(peerAddress, sourceJobId);
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorBrokerModule();
  }

}
