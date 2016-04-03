package org.nebulostore.crypto.session;

import java.io.Serializable;

import javax.crypto.KeyAgreement;

import com.google.inject.Inject;
import com.google.inject.name.Named;

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
import org.nebulostore.crypto.session.message.DHFinishMessage;
import org.nebulostore.crypto.session.message.DHGetSessionKeyMessage;
import org.nebulostore.crypto.session.message.DHGetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.DHLocalErrorMessage;
import org.nebulostore.crypto.session.message.DHOneAToBMessage;
import org.nebulostore.crypto.session.message.DHRemoteErrorMessage;
import org.nebulostore.crypto.session.message.DHTwoBToAMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.utils.Pair;

/**
 * @author lukaszsiczek
 */
public class SessionNegotiatorModule extends JobModule {

  private static final Logger LOGGER = Logger.getLogger(SessionNegotiatorModule.class);

  private enum ErrorNotificationMethod { NONE, LOCAL, REMOTE, ALL };

  private InitSessionNegotiatorModuleVisitor sessionNegotiatorModuleVisitor_ =
      new InitSessionNegotiatorModuleVisitor();
  private SessionContext initSessionContext_;

  private CommAddress myAddress_;
  private NetworkMonitor networkMonitor_;
  private EncryptionAPI encryptionAPI_;
  private String instancePrivateKeyId_;
  private String sessionId_;

  private CommAddress peerAddress_;
  private String localSourceJobId_;
  private String remoteSourceJobId_;
  private Serializable data_;
  private int ttl_;

  public SessionNegotiatorModule() {

  }

  public SessionNegotiatorModule(CommAddress peerAddress, String sourceJobId,
      Serializable data, int ttl) {
    peerAddress_ = peerAddress;
    localSourceJobId_ = sourceJobId;
    data_ = data;
    ttl_ = ttl;
  }

  @Inject
  public void setModuleDependencies(
      CommAddress myAddress,
      NetworkMonitor networkMonitor,
      EncryptionAPI encryptionAPI,
      @Named("InstancePrivateKeyId") String instancePrivateKeyId,
      SessionContext initSessionContext) {
    myAddress_ = myAddress;
    networkMonitor_ = networkMonitor;
    encryptionAPI_ = encryptionAPI;
    instancePrivateKeyId_ = instancePrivateKeyId;
    initSessionContext_ = initSessionContext;
  }

  protected class InitSessionNegotiatorModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      LOGGER.debug("Process JobInitMessage: " + message);
      initSessionContext_.acquireWriteLock();
      try {
        sessionId_ = initSessionContext_.tryAllocFreeSlot(new SessionObject(
            peerAddress_, localSourceJobId_, data_), ttl_);
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.REMOTE);
        return;
      } finally {
        initSessionContext_.releaseWriteLock();
      }
      try {
        Pair<KeyAgreement, DiffieHellmanInitPackage> firstStep =
            DiffieHellmanProtocol.firstStepDHKeyAgreement();
        EncryptedObject encryptedData = encryptionAPI_.encrypt(firstStep.getSecond(),
            instancePrivateKeyId_);
        initSessionContext_.tryGetInitSessionObject(sessionId_).setKeyAgreement(
            firstStep.getFirst());
        Message initSessionMessage = new DHOneAToBMessage(myAddress_, peerAddress_, sessionId_,
            getJobId(), ttl_, encryptedData);
        networkQueue_.add(initSessionMessage);
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.LOCAL);
      }
    }

    public void visit(DHOneAToBMessage message) {
      LOGGER.debug("Process InitSessionMessage: " + message);
      peerAddress_ = message.getSourceAddress();
      remoteSourceJobId_ = message.getSourceJobId();
      sessionId_ = message.getSessionId();
      if (!peerAddress_.equals(myAddress_)) {
        initSessionContext_.acquireWriteLock();
        try {
          initSessionContext_.allocFreeSlot(sessionId_, new SessionObject(peerAddress_),
              message.getTTL());
        } catch (SessionRuntimeException e) {
          endWithError(e, ErrorNotificationMethod.REMOTE);
          return;
        } finally {
          initSessionContext_.releaseWriteLock();
        }
      }
      String peerKeyId = networkMonitor_.getInstancePublicKeyId(peerAddress_);
      try {
        DiffieHellmanInitPackage diffieHellmanInitPackage = (DiffieHellmanInitPackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), peerKeyId);

        Pair<KeyAgreement, DiffieHellmanResponsePackage> secondStep =
            DiffieHellmanProtocol.secondStepDHKeyAgreement(diffieHellmanInitPackage);
        initSessionContext_.tryGetInitSessionObject(sessionId_).setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(secondStep.getFirst()));
        EncryptedObject encryptedData = encryptionAPI_.encrypt(secondStep.getSecond(),
            instancePrivateKeyId_);

        Message initSessionResponseMessage = new DHTwoBToAMessage(remoteSourceJobId_,
            myAddress_, peerAddress_, sessionId_, getJobId(), encryptedData);
        networkQueue_.add(initSessionResponseMessage);
        endWithSuccess();
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.REMOTE);
      }
    }

    public void visit(DHTwoBToAMessage message) {
      LOGGER.debug("Process InitSessionResponseMessage: " + message);
      remoteSourceJobId_ = message.getSourceJobId();
      SessionObject initSessionObject = null;
      initSessionContext_.acquireReadLock();
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(sessionId_);
      } catch (SessionRuntimeException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL);
        return;
      } finally {
        initSessionContext_.releaseReadLock();
      }
      String peerKeyId = networkMonitor_.getInstancePublicKeyId(peerAddress_);
      try {
        DiffieHellmanResponsePackage diffieHellmanResponsePackage = (DiffieHellmanResponsePackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), peerKeyId);

        KeyAgreement keyAgreement = DiffieHellmanProtocol.thirdStepDHKeyAgreement(
            initSessionObject.getKeyAgreement(), diffieHellmanResponsePackage);
        initSessionObject.setSessionKey(
            DiffieHellmanProtocol.fourthStepDHKeyAgreement(keyAgreement));

        DHFinishMessage initSessionEndMessage = new DHFinishMessage(initSessionObject);
        outQueue_.add(initSessionEndMessage);
        if (peerAddress_.equals(myAddress_)) {
          endWithSuccess();
        } else {
          endWithSuccessAndClear();
        }
      } catch (CryptoException e) {
        endWithErrorAndClear(e, ErrorNotificationMethod.ALL);
      }
    }

    public void visit(DHGetSessionKeyMessage message) {
      LOGGER.debug("Process GetSessionKeyMessage: " + message);
      peerAddress_ = message.getPeerAddress();
      localSourceJobId_ = message.getSourceJobId();
      sessionId_ = message.getSessionId();
      SessionObject initSessionObject = null;
      initSessionContext_.acquireReadLock();
      try {
        initSessionObject = initSessionContext_.tryGetInitSessionObject(sessionId_);
        if (initSessionObject.getSessionKey() == null) {
          throw new SessionRuntimeException("SessionKey null");
        }
      } catch (SessionRuntimeException e) {
        endWithError(e, ErrorNotificationMethod.LOCAL);
        return;
      } finally {
        initSessionContext_.releaseReadLock();
      }
      outQueue_.add(new DHGetSessionKeyResponseMessage(localSourceJobId_,
          message.getPeerAddress(), initSessionObject.getSessionKey(), sessionId_));
      endWithSuccessAndClear();
    }

    public void visit(DHRemoteErrorMessage message) {
      LOGGER.debug("Process InitSessionErrorMessage: " + message);
      endWithErrorAndClear(new SessionRuntimeException(message.getErrorMessage()),
          ErrorNotificationMethod.LOCAL);
    }

    private void removeSessionObjectFromContext(String id) {
      initSessionContext_.acquireWriteLock();
      try {
        initSessionContext_.tryRemoveInitSessionObject(id);
      } finally {
        initSessionContext_.releaseWriteLock();
      }
    }

    private void endWithErrorAndClear(Throwable e, ErrorNotificationMethod method) {
      try {
        removeSessionObjectFromContext(sessionId_);
      } catch (SessionRuntimeException exception) {
        LOGGER.error(exception.getMessage(), exception);
      }
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
      networkQueue_.add(new DHRemoteErrorMessage(destinationJobId, myAddress_,
          peerAddress_, e.getMessage()));
    }

    private void localErrorNotify(String destinationJobId, Throwable e) {
      outQueue_.add(new DHLocalErrorMessage(
          destinationJobId, e.getMessage(), peerAddress_));
    }

    private void endWithSuccessAndClear() {
      removeSessionObjectFromContext(sessionId_);
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
