package org.nebulostore.crypto.session;

import java.io.Serializable;

import javax.crypto.KeyAgreement;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.dh.DiffieHellmanInitPackage;
import org.nebulostore.crypto.dh.DiffieHellmanProtocol;
import org.nebulostore.crypto.dh.DiffieHellmanResponsePackage;
import org.nebulostore.crypto.session.message.GetSessionKeyMessage;
import org.nebulostore.crypto.session.message.GetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.InitSessionEndMessage;
import org.nebulostore.crypto.session.message.InitSessionEndWithErrorMessage;
import org.nebulostore.crypto.session.message.InitSessionErrorMessage;
import org.nebulostore.crypto.session.message.InitSessionMessage;
import org.nebulostore.crypto.session.message.InitSessionResponseMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public abstract class InitSessionNegotiatorModule extends JobModule {

  private static final Logger LOGGER = Logger.getLogger(InitSessionNegotiatorModule.class);

  private enum ErrorNotificationMethod { NONE, LOCAL, REMOTE, ALL };

  protected InitSessionNegotiatorModuleVisitor sessionNegotiatorModuleVisitor_;
  protected InitSessionContext initSessionContext_;

  private CommAddress myAddress_;
  private NetworkMonitor networkMonitor_;
  private EncryptionAPI encryptionAPI_;
  private String privateKeyPeerId_;

  private CommAddress peerAddress_;
  private String localSourceJobId_;
  private String remoteSourceJobId_;
  private Serializable data_;

  public InitSessionNegotiatorModule() {
  }

  public InitSessionNegotiatorModule(CommAddress peerAddress, String sourceJobId,
      Serializable data) {
    peerAddress_ = peerAddress;
    localSourceJobId_ = sourceJobId;
    data_ = data;
  }

  protected void setModuleDependencies(
      CommAddress myAddress,
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      String privateKeyPeerId,
      InitSessionContext initSessionContext) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    encryptionAPI_ = encryptionAPI;
    privateKeyPeerId_ = privateKeyPeerId;
    initSessionContext_ = initSessionContext;
  }

  protected abstract class InitSessionNegotiatorModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      LOGGER.debug("Process JobInitMessage: " + message);
      initSessionContext_.acquireWriteLock();
      try {
        initSessionContext_.tryAllocFreeSlot(peerAddress_,
            new InitSessionObject(peerAddress_, localSourceJobId_, data_));
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.LOCAL);
        return;
      } finally {
        initSessionContext_.releaseWriteLock();
      }
      String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress_);
      try {
        Pair<KeyAgreement, DiffieHellmanInitPackage> firstStep =
            DiffieHellmanProtocol.firstStepDHKeyAgreement();
        EncryptedObject encryptedData = encryptionAPI_.encrypt(firstStep.getSecond(), peerKeyId);
        initSessionContext_.tryGetInitSessionObject(
            peerAddress_).setKeyAgreement(firstStep.getFirst());
        Message initSessionMessage = createInitMessage(myAddress_, peerAddress_,
            getJobId(), encryptedData);
        networkQueue_.add(initSessionMessage);
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.LOCAL);
      }
    }

    public abstract InitSessionMessage createInitMessage(CommAddress myAddress,
        CommAddress peerAddress, String sourceJobId, EncryptedObject encryptedData);

    public void visit(InitSessionMessage message) {
      LOGGER.debug("Process InitSessionMessage: " + message);
      peerAddress_ = message.getSourceAddress();
      remoteSourceJobId_ = message.getSourceJobId();
      initSessionContext_.acquireWriteLock();
      try {
        initSessionContext_.tryAllocFreeSlot(peerAddress_, new InitSessionObject(peerAddress_));
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.REMOTE);
        return;
      } finally {
        initSessionContext_.releaseWriteLock();
      }
      try {
        DiffieHellmanInitPackage diffieHellmanInitPackage = (DiffieHellmanInitPackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);

        Pair<KeyAgreement, DiffieHellmanResponsePackage> secondStep =
            DiffieHellmanProtocol.secondStepDHKeyAgreement(diffieHellmanInitPackage);
        initSessionContext_.tryGetInitSessionObject(peerAddress_).setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(secondStep.getFirst()));
        String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress_);
        EncryptedObject encryptedData = encryptionAPI_.encrypt(secondStep.getSecond(), peerKeyId);

        Message initSessionResponseMessage = new InitSessionResponseMessage(
            message.getSourceJobId(), myAddress_, peerAddress_, getJobId(), encryptedData);
        networkQueue_.add(initSessionResponseMessage);
        endWithSuccess();
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.REMOTE);
      }
    }

    public void visit(InitSessionResponseMessage message) {
      LOGGER.debug("Process InitSessionResponseMessage: " + message);
      remoteSourceJobId_ = message.getSourceJobId();
      InitSessionObject initSessionObject = null;
      initSessionContext_.acquireReadLock();
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(peerAddress_);
      } catch (SessionRuntimeException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL);
        return;
      } finally {
        initSessionContext_.releaseReadLock();
      }
      try {
        DiffieHellmanResponsePackage diffieHellmanResponsePackage = (DiffieHellmanResponsePackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);

        KeyAgreement keyAgreement = DiffieHellmanProtocol.thirdStepDHKeyAgreement(
            initSessionObject.getKeyAgreement(), diffieHellmanResponsePackage);
        initSessionObject.setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(keyAgreement));

        InitSessionEndMessage initSessionEndMessage =
            new InitSessionEndMessage(initSessionObject, getJobId());
        outQueue_.add(initSessionEndMessage);
        endWithSuccessAndClear();
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL);
      }
    }

    protected void visit(GetSessionKeyMessage message) {
      LOGGER.debug("Process GetSessionKeyMessage: " + message);
      peerAddress_ = message.getPeerAddress();
      localSourceJobId_ = message.getSourceJobId();
      InitSessionObject initSessionObject = null;
      initSessionContext_.acquireReadLock();
      try {
        initSessionObject = initSessionContext_.
            tryGetInitSessionObject(peerAddress_);
        if (initSessionObject.getSessionKey() == null) {
          throw new SessionRuntimeException("SessionKey null");
        }
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.LOCAL);
        return;
      } finally {
        initSessionContext_.releaseReadLock();
      }
      outQueue_.add(new GetSessionKeyResponseMessage(localSourceJobId_,
          message.getPeerAddress(), initSessionObject.getSessionKey()));
      endWithSuccessAndClear();
    }

    public void visit(InitSessionErrorMessage message) {
      LOGGER.debug("Process InitSessionErrorMessage: " + message);
      endWithErrorAndClear(new SessionRuntimeException(message.getErrorMessage()),
          ErrorNotificationMethod.LOCAL);
    }

    private void removeSessionObjectFromContext(CommAddress peerAddress) {
      initSessionContext_.acquireWriteLock();
      try {
        initSessionContext_.tryRemoveInitSessionObject(peerAddress);
      } finally {
        initSessionContext_.releaseWriteLock();
      }
    }

    private void endWithErrorAndClear(Throwable e, ErrorNotificationMethod method) {
      removeSessionObjectFromContext(peerAddress_);
      endWithError(e, method);
    }

    private void endWithError(Throwable e, ErrorNotificationMethod method) {
      LOGGER.debug(e.getMessage(), e);
      switch (method) {
        case NONE:
          break;
        case LOCAL:
          localErrorNotify(localSourceJobId_, e);
          break;
        case REMOTE:
          remoteErrorNotify(remoteSourceJobId_, e);
          break;
        case ALL:
        default:
          localErrorNotify(localSourceJobId_, e);
          remoteErrorNotify(remoteSourceJobId_, e);
          break;
      }
      endJobModule();
    }

    private void remoteErrorNotify(String destinationJobId, Throwable e) {
      networkQueue_.add(new InitSessionErrorMessage(destinationJobId, myAddress_,
          peerAddress_, e.getMessage()));
    }

    private void localErrorNotify(String destinationJobId, Throwable e) {
      outQueue_.add(new InitSessionEndWithErrorMessage(
          destinationJobId, e.getMessage(), peerAddress_));
    }

    private void endWithSuccessAndClear() {
      removeSessionObjectFromContext(peerAddress_);
      endWithSuccess();
    }

    private void endWithSuccess() {
      LOGGER.debug("Process endWithSuccess peer: " + peerAddress_);
      endJobModule();
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(sessionNegotiatorModuleVisitor_);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
