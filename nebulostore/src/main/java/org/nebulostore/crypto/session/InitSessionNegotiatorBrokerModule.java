package org.nebulostore.crypto.session;

import java.io.Serializable;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.message.GetSessionKeyBrokerMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.InitSessionBrokerMessage;
import org.nebulostore.crypto.session.message.InitSessionMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;

/**
 * @author lukaszsiczek
 */
public class InitSessionNegotiatorBrokerModule extends InitSessionNegotiatorModule {

  public InitSessionNegotiatorBrokerModule() {
    super();
  }

  public InitSessionNegotiatorBrokerModule(CommAddress peerAddress, String sourceJobId,
      Serializable data) {
    super(peerAddress, sourceJobId, data);
  }

  @Inject
  public void setDependencies(
      CommAddress myAddress,
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      @Named("PrivateKeyPeerId") String privateKeyPeerId,
      @Named("BrokerInitSessionContext") InitSessionContext initSessionContext) {
    setModuleDependencies(myAddress, networkMonitor, encryptionAPI, privateKeyPeerId,
        initSessionContext);
    sessionNegotiatorModuleVisitor_ = new InitSessionNegotiatorBrokerModuleVisitor();
  }

  public class InitSessionNegotiatorBrokerModuleVisitor
    extends InitSessionNegotiatorModuleVisitor {

    @Override
    public InitSessionMessage createInitMessage(CommAddress myAddress, CommAddress peerAddress,
        String sourceJobId, EncryptedObject encryptedData) {
      return new InitSessionBrokerMessage(myAddress, peerAddress, sourceJobId, encryptedData);
    }

    public void visit(GetSessionKeyBrokerMessage message) {
      super.visit((GetSessionKeyMessage) message);
    }

  }
}
