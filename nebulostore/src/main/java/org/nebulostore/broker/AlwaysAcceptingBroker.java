package org.nebulostore.broker;

import java.util.Iterator;
import java.util.List;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;


/**
 * Broker is always a singleton job. See BrokerMessageForwarder.
 * Should be used only with BasicEncryptionAPI class
 * @author Bolek Kulbabinski
 */
public class AlwaysAcceptingBroker extends Broker {
  private static Logger logger_ = Logger.getLogger(AlwaysAcceptingBroker.class);

  private static final int TIMEOUT_SEC = 10;
  private static final int MAX_CONTRACTS = 10;
  /* Default offer of 10 MB */
  private static final int DEFAULT_OFFER = 10 * 1024;
  private final BrokerVisitor visitor_ = new BrokerVisitor();
  private NetworkMonitor networkMonitor_;

  @Inject
  public void setDependencies(NetworkMonitor networkMonitor) {
    networkMonitor_ = networkMonitor;
  }

  @Override
  protected void initModule() {
    subscribeForCommPeerFoundEvents();
  }

  protected void subscribeForCommPeerFoundEvents() {
    networkMonitor_.addContextChangeMessageGenerator(new MessageGenerator() {
      @Override
      public Message generate() {
        return new CommPeerFoundMessage(jobId_, null, null);
      }
    });
  }

  protected class BrokerVisitor extends MessageVisitor {
    public void visit(JobInitMessage message) {
      logger_.debug("Initialized");
    }

    public void visit(ContractOfferMessage message) throws CryptoException {
      // Accept every offer!
      logger_.debug("Accepting offer from: " +
          message.getSourceAddress());
      // TODO(bolek): Should we accept same offer twice?
      networkQueue_.add(new OfferReplyMessage(message.getId(), message.getSourceAddress(),
          message.getEncryptedContract(), true));
    }

    public void visit(OfferReplyMessage message) throws CryptoException {
      Contract contract = (Contract)
          encryptionAPI_.decryptWithSessionKey(message.getEncryptedContract(), null);
      if (message.getResult()) {
        // Offer was accepted, add new replica to our DHT entry.
        logger_.debug("Peer " +
            message.getSourceAddress() + " accepted our offer.");
        context_.addContract(contract);

        try {
          updateReplicationGroups(TIMEOUT_SEC);
        } catch (NebuloException e) {
          logger_.warn("Unsuccessful DHT update.");
        }
      } else {
        logger_.debug("Peer " +
            message.getSourceAddress() + " rejected our offer.");
      }
      context_.removeOffer(message.getSourceAddress());
    }

    public void visit(CommPeerFoundMessage message) throws CryptoException {
      logger_.debug("Found new peer.");
      if (context_.getReplicas().length + context_.getNumberOfOffers() < MAX_CONTRACTS) {
        List<CommAddress> knownPeers = networkMonitor_.getKnownPeers();
        Iterator<CommAddress> iterator = knownPeers.iterator();
        while (iterator.hasNext()) {
          CommAddress address = iterator.next();
          if (!address.equals(myAddress_) &&
              context_.getUserContracts(address) == null && !context_.containsOffer(address)) {
            // Send offer to new peer (10MB by default).
            logger_.debug("Sending offer to " +
                address);
            Contract offer = new Contract(myAddress_, address, DEFAULT_OFFER);
            EncryptedObject encryptedOffer = encryptionAPI_.encryptWithSessionKey(offer, null);
            networkQueue_.add(
                new ContractOfferMessage(CryptoUtils.getRandomString(), address, encryptedOffer));
            context_.addContractOffer(address, offer);
            break;
          }
        }
      }
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
