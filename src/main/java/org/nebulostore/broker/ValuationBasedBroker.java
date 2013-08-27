package org.nebulostore.broker;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.broker.ContractsSelectionAlgorithm.OfferResponse;
import org.nebulostore.broker.messages.BreakContractMessage;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.ImproveContractsMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.communication.address.CommAddress;
import org.nebulostore.communication.messages.ErrorCommMessage;
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
      @Named("broker.default-contract-size-kb") int defaultContractSizeKb,
      ContractsSelectionAlgorithm contractsSelectionAlgorithm,
      @Named("broker.max-contracts-multiplicity") int maxContractsMultiplicity,
      Timer timer) {
    replicationGroupUpdateTimeout_ = replicationGroupUpdateTimeout;
    networkMonitor_ = networkMonitor;
    contractImprovementPeriod_ = contractImprovementPeriod;
    contractImprovementDelay_ = contractImprovementDelay;
    defaultContractSizeKb_ = defaultContractSizeKb;
    contractsSelectionAlgorithm_ = contractsSelectionAlgorithm;
    maxContractsMultiplicity_ = maxContractsMultiplicity;
    timer_ = timer;
  }

  // Injected constants.
  private static int replicationGroupUpdateTimeout_;
  private static int contractImprovementPeriod_;
  private static int contractImprovementDelay_;
  private static int defaultContractSizeKb_;
  private static int maxContractsMultiplicity_;

  private Timer timer_;
  private ContractsSelectionAlgorithm contractsSelectionAlgorithm_;

  public void updateReplicationGroups() {
    // todo(szm): one group for now
    ReplicationGroup currGroup = new ReplicationGroup(BrokerContext.getInstance().getReplicas(),
        new BigInteger("0"), new BigInteger("1000000"));
    PutKeyModule module = new PutKeyModule(currGroup, outQueue_);

    try {
      module.getResult(replicationGroupUpdateTimeout_);
    } catch (NebuloException exception) {
      logger_.warn("Unsuccessful DHT update.");
    }
  }

  private final BrokerVisitor visitor_ = new BrokerVisitor();

  /**
   * Visitor.
   */
  public class BrokerVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
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
      return null;
    }

    public Void visit(BreakContractMessage message) {
      logger_.debug("Broken: " + message.getContract().toString());
      BrokerContext.getInstance().remove(message.getContract());
      return null;
    }

    public Void visit(ImproveContractsMessage message) {
      logger_.debug("Improving contracts...");
      jobId_ = message.getId();

      Set<Contract> possibleContracts = new HashSet<Contract>();
      Set<CommAddress> randomPeersSample = networkMonitor_.getRandomPeersSample();

      // todo(szm): temporarily using gossiped random peers sample
      // todo(szm): choosing peers to offer contracts should be somewhere different
      for (CommAddress commAddress : randomPeersSample) {
        if (BrokerContext.getInstance().getNumberOfContractsWith(commAddress) <
            maxContractsMultiplicity_) {
          possibleContracts.add(new Contract(myAddress_, commAddress, defaultContractSizeKb_));
        }
      }

      try {
        ContractsSet currentContracts = BrokerContext.getInstance().acquireReadAccessToContracts();

        if (possibleContracts.isEmpty()) {
          logger_.debug("No possible new contracts.");
          endJobModule();
        } else {
          Contract toOffer = contractsSelectionAlgorithm_
              .chooseContractToOffer(possibleContracts, currentContracts);
          networkQueue_.add(new ContractOfferMessage(getJobId(), toOffer.getPeer(), toOffer));
          // TODO(szm): timeout
        }
      } finally {
        BrokerContext.getInstance().disposeReadAccessToContracts();
      }
      return null;
    }

    public Void visit(OfferReplyMessage message) {
      if (message.getResult()) {
        logger_.debug("Contract concluded: " +
          message.getContract().toLocalAndRemoteSwapped().toString());
        BrokerContext.getInstance().addContract(message.getContract());

        // todo(szm): przydzielanie przestrzeni adresowej do kontraktow
        // todo(szm): z czasem coraz rzadziej polepszam kontrakty
        ReplicationGroup currGroup = new ReplicationGroup(
            BrokerContext.getInstance().getReplicas(), new BigInteger("0"), new BigInteger(
                "1000000"));
        PutKeyModule module = new PutKeyModule(currGroup, outQueue_);

        try {
          module.getResult(replicationGroupUpdateTimeout_);
        } catch (NebuloException exception) {
          logger_.warn("Unsuccessful DHT update after contract conclusion.");
        }
      } else {
        logger_.debug("Contract not concluded: " + message.getContract().toString());
      }
      // todo(szm): timeouts
      return null;
    }

    public Void visit(ContractOfferMessage message) {
      ContractsSet contracts = BrokerContext.getInstance().acquireReadAccessToContracts();
      OfferResponse response;
      try {
        response = contractsSelectionAlgorithm_.responseToOffer(
            message.getContract(), contracts);
      } finally {
        BrokerContext.getInstance().disposeReadAccessToContracts();
      }
      if (response.responseAnswer_) {
        logger_.debug("Concluding contract: " + message.getContract().toString());
        BrokerContext.getInstance().addContract(message.getContract().toLocalAndRemoteSwapped());
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            message.getContract(), true));
        for (Contract contract : response.contractsToBreak_) {
          sendBreakContractMessage(contract);
        }

        updateReplicationGroups();
      } else {
        networkQueue_.add(new OfferReplyMessage(getJobId(), message.getSourceAddress(),
            message.getContract(), false));
      }
      return null;
    }

    public Void visit(ErrorCommMessage message) {
      logger_.debug("Received: " + message);
      return null;
    }

  }

  private void sendBreakContractMessage(Contract contract) {
    networkQueue_.add(new BreakContractMessage(null, contract.getPeer(), contract));
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
