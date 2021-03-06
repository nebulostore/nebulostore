package org.nebulostore.api;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import com.google.inject.Provider;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetFullObjectModule.STATE;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.crypto.DecryptWrapper;
import org.nebulostore.crypto.EncryptionAPI;
import org.nebulostore.crypto.session.SessionNegotiatorModule;
import org.nebulostore.crypto.session.message.DHFinishMessage;
import org.nebulostore.crypto.session.message.DHLocalErrorMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Bolek Kulbabinski
 *
 * @param <V>
 *          Returning type.
 */
public abstract class GetModule<V> extends ReturningJobModule<V> {
  private static Logger logger_ = Logger.getLogger(GetModule.class);

  private static final long REPLICA_WAIT_MILLIS = 5000L;

  protected Provider<Timer> timerProvider_;
  protected EncryptionAPI encryption_;
  protected DecryptWrapper decryptWrapper_;
  protected CommAddress myAddress_;
  protected List<String> currentVersions_;

  protected NebuloAddress address_;
  protected Map<String, SecretKey> sessionKeys_ = new HashMap<String, SecretKey>();
  protected NetworkMonitor networkMonitor_;

  public void setDependencies(Provider<Timer> timerProvider, EncryptionAPI encryptionAPI,
      NetworkMonitor networkMonitor) {
    timerProvider_ = timerProvider;
    encryption_ = encryptionAPI;
    networkMonitor_ = networkMonitor;
  }

  public void fetchObject(NebuloAddress address, DecryptWrapper decryptWrapper) {
    address_ = checkNotNull(address);
    decryptWrapper_ = decryptWrapper;
    runThroughDispatcher();
  }

  protected abstract class GetModuleVisitor extends MessageVisitor {

    protected List<CommAddress> replicationGroupList_;
    protected final Set<CommAddress> waitingForReplicators_ = new HashSet<>();
    protected STATE state_ = STATE.INIT;

    public abstract void visit(JobInitMessage message);

    public abstract void visit(SendObjectMessage message);

    public abstract void visit(TimeoutMessage message);

    public void visit(DHFinishMessage message) {
      logger_.debug("Process " + message);
      sessionKeys_.put(message.getSessionId(), message.getSessionKey());
      networkQueue_.add(new GetObjectMessage(CryptoUtils.getRandomId().toString(), myAddress_,
          message.getPeerAddress(), address_.getObjectId(), jobId_, message.getSessionId()));
    }

    public void visit(DHLocalErrorMessage message) {
      logger_.debug("Process InitSessionEndWithErrorMessage " + message);
      failReplicator(message.getPeerAddress());
    }

    public void visit(ReplicatorErrorMessage message) {
      logger_.warn("ReplicatorErrorMessage received.");
      if (state_ == STATE.REPLICA_FETCH) {
        failReplicator(message.getSourceAddress());
      } else {
        incorrectState(state_.name(), message);
      }
    }

    protected abstract void failReplicator(CommAddress replicator);

    protected void queryNextReplicas() {
      if (replicationGroupList_.isEmpty()) {
        endWithError(new NebuloException("Not enough replicas responded in time."));
      } else {
        Iterator<CommAddress> iterator = replicationGroupList_.iterator();
        while (iterator.hasNext()) {
          CommAddress replicator = iterator.next();
          if (!waitingForReplicators_.contains(replicator)) {
            logger_.debug("Querying replica (" + replicator + ")");
            waitingForReplicators_.add(replicator);
            iterator.remove();
            startSessionAgreement(replicator);
          }
        }
        timerProvider_.get().schedule(jobId_, REPLICA_WAIT_MILLIS, STATE.REPLICA_FETCH.name());
      }
    }

    /**
     * Check object version received in SendObjectMessage and update it if necessary.
     *
     * @param message
     * @return true if the version was changed to newer, false otherwise
     * @throws NebuloException
     *           when received fragment is not usable because of incorrect version
     */
    protected boolean checkVersion(SendObjectMessage message) throws NebuloException {
      List<String> remoteVersions = message.getVersions();
      logger_.debug("Current versions: " + currentVersions_ + "\n Remote versions: " +
          remoteVersions);
      if (currentVersions_ == null) {
        currentVersions_ = remoteVersions;
        return true;
      }
      if (remoteVersions.size() >= currentVersions_.size() &&
          remoteVersions.subList(0, currentVersions_.size()).equals(currentVersions_)) {
        if (remoteVersions.size() > currentVersions_.size()) {
          currentVersions_ = remoteVersions;
          return true;
        }
      } else {
        throw new NebuloException("Received an outdated object fragment.");
      }

      return false;
    }

    protected void incorrectState(String stateName, Message message) {
      logger_.warn(message.getClass().getSimpleName() + " received in state " + stateName);
    }

    private void startSessionAgreement(CommAddress replicator) {
      SessionNegotiatorModule initSessionNegotiatorModule =
          new SessionNegotiatorModule(replicator, getJobId(), null, 1);
      outQueue_.add(new JobInitMessage(initSessionNegotiatorModule));
    }

    protected EncryptedObject decryptWithSessionKey(EncryptedObject cipher, String sessionId)
        throws CryptoException {
      return (EncryptedObject) encryption_.decryptWithSessionKey(cipher,
          sessionKeys_.remove(sessionId));
    }
  }
}
