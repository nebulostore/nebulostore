package org.nebulostore.broker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.KeyAgreement;
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
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.ImproveContractsMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.dh.DiffieHellmanInitPackage;
import org.nebulostore.crypto.dh.DiffieHellmanProtocol;
import org.nebulostore.crypto.dh.DiffieHellmanResponsePackage;
import org.nebulostore.crypto.message.InitSessionErrorMessage;
import org.nebulostore.crypto.message.InitSessionMessage;
import org.nebulostore.crypto.message.InitSessionResponseMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.Timer;
import org.nebulostore.utils.Pair;

/**
 * Module that initializes Broker and provides methods shared by modules in broker package.
 *
 * @author bolek, szymonmatejczyk
 */
public class ValuationBasedBroker extends Broker {
  private static Logger logger_ = Logger.getLogger(ValuationBasedBroker.class);
  private static final String CONFIGURATION_PREFIX = "broker.";
  private Map<CommAddress, KeyAgreement> keyAgreements_ = new HashMap<CommAddress, KeyAgreement>();
  private Map<CommAddress, SecretKey> sessionKeys_ = new HashMap<CommAddress, SecretKey>();
  private Map<CommAddress, Contract> workingOffer_ = new HashMap<CommAddress, Contract>();
  private static final Contract CONTRACT_INIT_BY_OTHER_PEER = null;

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
      //FIXME concurrentModificationException was seen here
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
          if (!workingOffer_.containsKey(toOffer.getPeer())) {
            startKeyAgreement(toOffer);
          } else {
            logger_.debug("Already processing contract " + toOffer);
          }
        }
      } finally {
        context_.disposeReadAccessToContracts();
      }
    }

    public void visit(InitSessionMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      if (workingOffer_.containsKey(peerAddress)) {
        logger_.debug("Already processing contract with peer " + peerAddress);
        networkQueue_.add(new InitSessionErrorMessage(getJobId(), myAddress_, peerAddress));
        return;
      }
      workingOffer_.put(peerAddress, CONTRACT_INIT_BY_OTHER_PEER);
      try {
        DiffieHellmanInitPackage diffieHellmanInitPackage = (DiffieHellmanInitPackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);

        Pair<KeyAgreement, DiffieHellmanResponsePackage> secondStep =
            DiffieHellmanProtocol.secondStepDHKeyAgreement(diffieHellmanInitPackage);
        sessionKeys_.put(peerAddress, DiffieHellmanProtocol.fourthStepDHKeyAgreement(
            secondStep.getFirst()));
        String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress);
        EncryptedObject encryptedData = encryptionAPI_.encrypt(secondStep.getSecond(), peerKeyId);

        Message outputMessage = new InitSessionResponseMessage(getJobId(), myAddress_, peerAddress,
            encryptedData);
        networkQueue_.add(outputMessage);
      } catch (CryptoException e) {
        logger_.error(e.getMessage(), e);
        clearInitSessionVariables(peerAddress);
        networkQueue_.add(new InitSessionErrorMessage(getJobId(), myAddress_, peerAddress));
      }
    }

    public void visit(InitSessionResponseMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      if (!workingOffer_.containsKey(peerAddress)) {
        logger_.debug("Wrong Protocol order. Peer: " + peerAddress);
        networkQueue_.add(new InitSessionErrorMessage(getJobId(), myAddress_, peerAddress));
        return;
      }
      try {
        DiffieHellmanResponsePackage diffieHellmanResponsePackage = (DiffieHellmanResponsePackage)
            encryptionAPI_.decrypt(message.getEncryptedData(), privateKeyPeerId_);

        KeyAgreement keyAgreement = DiffieHellmanProtocol.thirdStepDHKeyAgreement(
            keyAgreements_.get(peerAddress), diffieHellmanResponsePackage);
        keyAgreements_.remove(peerAddress);
        SecretKey secretKey = DiffieHellmanProtocol.fourthStepDHKeyAgreement(keyAgreement);
        Contract contract = workingOffer_.get(peerAddress);
        EncryptedObject offer = encryptionAPI_.encryptWithSessionKey(contract, secretKey);
        sessionKeys_.put(peerAddress, secretKey);
        networkQueue_.add(new ContractOfferMessage(getJobId(), peerAddress, offer));
      } catch (CryptoException e) {
        logger_.error(e.getMessage(), e);
        clearInitSessionVariables(peerAddress);
        networkQueue_.add(new InitSessionErrorMessage(getJobId(), myAddress_, peerAddress));
      }
    }

    public void visit(InitSessionErrorMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      logger_.debug("Init Session Error Message from peer " + peerAddress);
      clearInitSessionVariables(peerAddress);
    }

    public void visit(OfferReplyMessage message) {
      CommAddress peerAddress = message.getSourceAddress();
      Contract contract = null;
      try {
        SecretKey secretKey = sessionKeys_.remove(peerAddress);
        contract = (Contract)
            encryptionAPI_.decryptWithSessionKey(message.getEncryptedContract(), secretKey);
      } catch (NullPointerException | CryptoException e) {
        logger_.error(e.getMessage(), e);
        clearInitSessionVariables(peerAddress);
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
      ContractsSet contracts = context_.acquireReadAccessToContracts();
      OfferResponse response;
      Contract offer = null;
      EncryptedObject encryptedOffer = null;
      try {
        SecretKey secretKey = sessionKeys_.remove(peerAddress);
        offer = (Contract) encryptionAPI_.decryptWithSessionKey(
            message.getEncryptedContract(), secretKey);
        offer.toLocalAndRemoteSwapped();
        encryptedOffer = encryptionAPI_.encryptWithSessionKey(offer, secretKey);
      } catch (NullPointerException | CryptoException e) {
        logger_.error(e.getMessage(), e);
        context_.disposeReadAccessToContracts();
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            encryptedOffer, false));
        return;
      } finally {
        clearInitSessionVariables(peerAddress);
      }
      if (context_.getContractsRealSize() > spaceContributedKb_ ||
          context_.getNumberOfContractsWith(offer.getPeer()) >=
          maxContractsMultiplicity_) {
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            encryptedOffer, false));
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
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            encryptedOffer, true));
        for (Contract contract : response.contractsToBreak_) {
          sendBreakContractMessage(contract);
        }
        try {
          updateReplicationGroups(replicationGroupUpdateTimeout_);
        } catch (NebuloException e) {
          logger_.warn("Unsuccessful DHT update.");
        }
      } else {
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            encryptedOffer, false));
      }
    }

    public void visit(ErrorCommMessage message) {
      logger_.debug("Received: " + message);
    }

  }

  private void startKeyAgreement(Contract toOffer) {
    CommAddress peerAddress = toOffer.getPeer();
    workingOffer_.put(peerAddress, toOffer);
    try {
      String peerKeyId = networkMonitor_.getPeerPublicKeyId(peerAddress);
      Pair<KeyAgreement, DiffieHellmanInitPackage> firstStep =
          DiffieHellmanProtocol.firstStepDHKeyAgreement();
      EncryptedObject encryptedData = encryptionAPI_.encrypt(firstStep.getSecond(), peerKeyId);
      keyAgreements_.put(peerAddress, firstStep.getFirst());
      Message message = new InitSessionMessage(getJobId(), myAddress_, peerAddress, encryptedData);
      networkQueue_.add(message);
    } catch (CryptoException e) {
      logger_.error(e.getMessage(), e);
      clearInitSessionVariables(peerAddress);
    }
  }

  private void clearInitSessionVariables(CommAddress peerAddress) {
    keyAgreements_.remove(peerAddress);
    workingOffer_.remove(peerAddress);
    sessionKeys_.remove(peerAddress);
  }

  private void sendBreakContractMessage(Contract contract) {
    networkQueue_.add(new BreakContractMessage(null, contract.getPeer(), contract));
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
