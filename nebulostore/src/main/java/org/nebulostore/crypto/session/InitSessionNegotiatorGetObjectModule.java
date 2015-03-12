package org.nebulostore.crypto.session;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.GetSessionKeyGetObjectMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.InitSessionGetObjectMessage;
import org.nebulostore.crypto.session.message.InitSessionMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * @author lukaszsiczek
 */
public class InitSessionNegotiatorGetObjectModule extends InitSessionNegotiatorModule {

  public InitSessionNegotiatorGetObjectModule() {
    super();
  }

  public InitSessionNegotiatorGetObjectModule(CommAddress peerAddress, String sourceJobId) {
    super(peerAddress, sourceJobId, null);
  }

  @Inject
  public void setDependencies(
      CommAddress myAddress,
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      @Named("GetObjectInitSessionContext") InitSessionContext initSessionContext) {
    setModuleDependencies(myAddress, networkMonitor, encryptionAPI, privateKeyPeerId,
        initSessionContext);
    sessionNegotiatorModuleVisitor_ = new InitSessionNegotiatorGetObjectModuleVisitor();
  }

  public class InitSessionNegotiatorGetObjectModuleVisitor
    extends InitSessionNegotiatorModuleVisitor {

    @Override
    public InitSessionMessage createInitMessage(CommAddress myAddress, CommAddress peerAddress,
        String sourceJobId, EncryptedObject encryptedData) {
      return new InitSessionGetObjectMessage(myAddress, peerAddress, sourceJobId, encryptedData);
    }

    public void visit(GetSessionKeyGetObjectMessage message) {
      super.visit((GetSessionKeyMessage) message);
    }

  }
}
