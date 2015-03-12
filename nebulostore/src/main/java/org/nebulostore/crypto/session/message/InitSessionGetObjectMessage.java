package org.nebulostore.crypto.session.message;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.InitSessionNegotiatorGetObjectModule;

/**
 * @author lukaszsiczek
 */
public class InitSessionGetObjectMessage extends InitSessionMessage {

  private static final long serialVersionUID = -5104465269696783407L;

  public InitSessionGetObjectMessage(CommAddress sourceAddress, CommAddress destAddress,
      String sourceJobId, EncryptedObject data) {
    super(sourceAddress, destAddress, sourceJobId, data);
  }

  @Override
  public JobModule getHandler() {
    return new InitSessionNegotiatorGetObjectModule();
  }

}
