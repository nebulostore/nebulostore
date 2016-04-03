package org.nebulostore.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.crypto.SecretKey;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.broker.ContractsSelectionAlgorithm.OfferResponse;
import org.nebulostore.broker.messages.BreakContractMessage;
import org.nebulostore.broker.messages.CheckContractMessage;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.ImproveContractsMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.session.SessionNegotiatorModule;
import org.nebulostore.crypto.session.message.DHFinishMessage;
import org.nebulostore.crypto.session.message.DHGetSessionKeyMessage;
import org.nebulostore.crypto.session.message.DHGetSessionKeyResponseMessage;
import org.nebulostore.crypto.session.message.DHLocalErrorMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.Timer;

/**
 * Module that initializes Broker and provides methods shared by modules in broker package.
 *
 * @author bolek, szymonmatejczyk
 */
public class ValuationBasedBroker extends Broker {
  private static Logger logger_ = Logger.getLogger(ValuationBasedBroker.class);
  private static final String CONFIGURATION_PREFIX = "broker.";
  private final Map<String, SecretKey> sessionKeys_ = new HashMap<String, SecretKey>();
  private final Map<String, EncryptedObject> contractOffer_ =
      new HashMap<String, EncryptedObject>();

  public ValuationBasedBroker() {
  }

  protected ValuationBasedBroker(String jobId) {
    jobId_ = jobId;
  }

  @Inject
  public void setDependencies(
      @Named("replicator.replication-group-update-timeout")
      int replicationGroupUpdateTimeout,
      NetworkMonitor networkMonitor,
      @Named(CONFIGURATION_PREFIX + "contracts-improvement-period-sec")
      int contractImprovementPeriod,
      @Named(CONFIGURATION_PREFIX + "contracts-improvement-delay-sec")
      int contractImprovementDelay,
      @Named(CONFIGURATION_PREFIX + "default-contract-size-kb") int defaultContractSizeKb,
      ContractsSelectionAlgorithm contractsSelectionAlgorithm,
      @Named(CONFIGURATION_PREFIX + "max-contracts-multiplicity") int maxContractsMultiplicity,
      @Named(CONFIGURATION_PREFIX + "space-contributed-kb") int spaceContributedKb,
      Timer timer) {
    replicationGroupUpdateTimeout_ = replicationGroupUpdateTimeout;
    networkMonitor_ = networkMonitor;
    contractImprovementPeriod_ = contractImprovementPeriod;
    contractImprovementDelay_ = contractImprovementDelay;
    defaultContractSizeKb_ = defaultContractSizeKb;
    contractsSelectionAlgorithm_ = contractsSelectionAlgorithm;
    maxContractsMultiplicity_ = maxContractsMultiplicity;
    spaceContributedKb_ = spaceContributedKb;
    timer_ = timer;
  }

  // Injected constants.
  private int replicationGroupUpdateTimeout_;
  private int contractImprovementPeriod_;
  private int contractImprovementDelay_;
  private int defaultContractSizeKb_;
  private int maxContractsMultiplicity_;
  private int spaceContributedKb_;

  private Timer timer_;
  private ContractsSelectionAlgorithm contractsSelectionAlgorithm_;

  private final BrokerVisitor visitor_ = new BrokerVisitor();

  public class BrokerVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      logger_.debug("Initialized.");
      // setting contracts improvement, when a new peer is discovered
      MessageGenerator contractImrovementMessageGenerator = new MessageGenerator() {
        @Override
        public Message generate() {
          return new ImproveContractsMessage(jobId_);
        }
      };
      networkMonitor_.addContextChangeMessageGenerator(
          contractImrovementMessageGenerator);

      // setting periodical contracts improvement
      timer_.scheduleRepeated(new ImproveContractsMessage(jobId_),
          contractImprovementDelay_ * 1000,
          contractImprovementPeriod_ * 1000);
    }

    public void visit(BreakContractMessage message) {
      logger_.debug("Broken: " + message.getContract().toString());
      context_.remove(message.getContract());
    }

    public void visit(ImproveContractsMessage message) {
      logger_.debug("Improving contracts...");

      Set<Contract> possibleContracts = new HashSet<Contract>();
      Set<CommAddress> randomPeersSample = networkMonitor_.getRandomPeersSample();

      if (context_.getContractsRealSize() > spaceContributedKb_) {
        logger_.debug("Contributed size fully utilized.");
        return;
      }

      // todo(szm): temporarily using gossiped random peers sample
      // todo(szm): choosing peers to offer contracts should be somewhere different
      for (CommAddress commAddress : randomPeersSample) {
        if (context_.getNumberOfContractsWith(commAddress) <
            maxContractsMultiplicity_) {
          possibleContracts.add(new Contract(myAddress_, commAddress, defaultContractSizeKb_));
        }
      }

      try {
        ContractsSet currentContracts = context_.acquireReadAccessToContracts();

        if (possibleContracts.isEmpty()) {
          logger_.debug("No possible new contracts.");
        } else {
          Contract toOffer = contractsSelectionAlgorithm_
              .chooseContractToOffer(possibleContracts, currentContracts);
          // TODO(szm): timeout
          startSessionAgreement(toOffer);
        }
      } finally {
        context_.disposeReadAccessToContracts();
      }
    }

    public void visit(DHFinishMessage message) {
      logger_.debug("Process " + message);
      CommAddress peerAddress = message.getPeerAddress();
      SecretKey sessionKey = message.getSessionKey();
      sessionKeys_.put(message.getSessionId(), sessionKey);
      try {
        EncryptedObject offer = encryptionAPI_.encryptWithSessionKey(message.getData(), sessionKey);
        networkQueue_.add(new ContractOfferMessage(getJobId(), peerAddress, offer,
            message.getSessionId()));
      } catch (CryptoException e) {
        clearInitSessionVariables(message.getSessionId());
      }
    }

    public void visit(DHLocalErrorMessage message) {
      logger_.debug("InitSessionEndWithErrorMessage " + message.getErrorMessage());
    }

    public void visit(OfferReplyMessage message) {
      Contract contract = null;
      SecretKey secretKey = sessionKeys_.remove(message.getSessionId());
      try {
        contract = (Contract)
            encryptionAPI_.decryptWithSessionKey(message.getEncryptedContract(), secretKey);
      } catch (CryptoException | NullPointerException e) {
        logger_.error(e.getMessage(), e);
        return;
      }
      contract.toLocalAndRemoteSwapped();
      if (message.getResult()) {
        logger_.debug("Contract concluded: " + contract);
        context_.addContract(contract);

        // todo(szm): przydzielanie przestrzeni adresowej do kontraktow
        // todo(szm): z czasem coraz rzadziej polepszam kontrakty
        try {
          updateReplicationGroups(replicationGroupUpdateTimeout_);
        } catch (NebuloException e) {
          logger_.warn("Unsuccessful DHT update after contract conclusion.");
        }
      } else {
        logger_.debug("Contract not concluded: " + contract);
      }
      // todo(szm): timeouts
    }

    public void visit(ContractOfferMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      String sessionId = message.getSessionId();
      contractOffer_.put(sessionId, message.getEncryptedContract());
      outQueue_.add(new DHGetSessionKeyMessage(peerAddress, getJobId(), sessionId));
    }

    public void visit(DHGetSessionKeyResponseMessage message) {
      CommAddress peerAddress = message.getPeerAddress();
      ContractsSet contracts = context_.acquireReadAccessToContracts();
      String sessionId = message.getSessionId();
      OfferResponse response;
      Contract offer = null;
      EncryptedObject encryptedOffer = null;
      try {
        offer = (Contract) encryptionAPI_.decryptWithSessionKey(
            contractOffer_.remove(sessionId), message.getSessionKey());
        offer.toLocalAndRemoteSwapped();
        encryptedOffer = encryptionAPI_.encryptWithSessionKey(offer, message.getSessionKey());
      } catch (CryptoException e) {
        logger_.error(e.getMessage(), e);
        context_.disposeReadAccessToContracts();
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress,
            encryptedOffer, sessionId, false));
        return;
      } finally {
        clearInitSessionVariables(sessionId);
      }
      if (context_.getContractsRealSize() > spaceContributedKb_ ||
          context_.getNumberOfContractsWith(offer.getPeer()) >=
          maxContractsMultiplicity_) {
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress,
            encryptedOffer, sessionId, false));
        context_.disposeReadAccessToContracts();
        return;
      }
      try {
        response = contractsSelectionAlgorithm_.responseToOffer(offer, contracts);
      } finally {
        context_.disposeReadAccessToContracts();
      }
      if (response.responseAnswer_) {
        logger_.debug("Concluding contract: " + offer);
        context_.addContract(offer);
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress,
            encryptedOffer, sessionId, true));
        for (Contract contract : response.contractsToBreak_) {
          sendBreakContractMessage(contract);
        }
        try {
          updateReplicationGroups(replicationGroupUpdateTimeout_);
        } catch (NebuloException e) {
          logger_.warn("Unsuccessful DHT update.");
        }
      } else {
        networkQueue_.add(new OfferReplyMessage(getJobId(), peerAddress,
            encryptedOffer, sessionId, false));
      }
    }

    public void visit(CheckContractMessage message) {
      logger_.debug("CheckContractMessage Peer " + message.getContractPeer());
      context_.acquireReadAccessToContracts();
      boolean result = false;
      try {
        List<Contract> contracts = context_.getContractList().get(message.getContractPeer());
        result = (contracts != null && !contracts.isEmpty()) ||
            message.getContractPeer().equals(myAddress_);
      } finally {
        context_.disposeReadAccessToContracts();
      }
      outQueue_.add(message.getResponse(result));
    }

    public void visit(ErrorCommMessage message) {
      logger_.debug("Received: " + message);
    }

  }

  private void startSessionAgreement(Contract offer) {
    SessionNegotiatorModule initSessionNegotiatorModule =
        new SessionNegotiatorModule(offer.getPeer(), getJobId(), offer, 1);
    outQueue_.add(new JobInitMessage(initSessionNegotiatorModule));
  }

  private void clearInitSessionVariables(String sessionId) {
    sessionKeys_.remove(sessionId);
    contractOffer_.remove(sessionId);
  }

  private void sendBreakContractMessage(Contract contract) {
    networkQueue_.add(new BreakContractMessage(null, contract.getPeer(), contract));
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
