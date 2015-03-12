package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorGetObjectModule;

/**
 * @author lukaszsiczek
 */
public class GetSessionKeyGetObjectMessage extends GetSessionKeyMessage {

  private static final long serialVersionUID = 6827128138911976671L;

  public GetSessionKeyGetObjectMessage(CommAddress peerAddress, String jobId) {
    super(peerAddress, jobId);
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorGetObjectModule();
  }

}
