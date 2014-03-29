package org.nebulostore.broker;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.api.PutKeyModule;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.broker.messages.ContractOfferMessage;
import org.nebulostore.broker.messages.OfferReplyMessage;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;


/**
 * Broker is always a singleton job. See BrokerMessageForwarder.
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

  /**
   * Visitor.
   */
  protected class BrokerVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      logger_.debug("Initialized");
      return null;
    }

    public Void visit(ContractOfferMessage message) {
      // Accept every offer!
      logger_.debug("Accepting offer from: " +
          message.getSourceAddress());
      // TODO(bolek): Should we accept same offer twice?
      networkQueue_.add(new OfferReplyMessage(message.getId(), message.getSourceAddress(), message
          .getContract(), true));
      return null;
    }

    public Void visit(OfferReplyMessage message) {
      if (message.getResult()) {
        // Offer was accepted, add new replica to our DHT entry.
        logger_.debug("Peer " +
            message.getSourceAddress() + " accepted our offer.");
        context_.addContract(message.getContract());

        ReplicationGroup currGroup = new ReplicationGroup(
          context_.getReplicas(), BigInteger.ZERO, new BigInteger("1000000"));
        PutKeyModule module = new PutKeyModule(currGroup, outQueue_);

        try {
          module.getResult(TIMEOUT_SEC);
        } catch (NebuloException exception) {
          logger_.warn("Unsuccessful DHT update.");
        }
      } else {
        logger_.debug("Peer " +
            message.getSourceAddress() + " rejected our offer.");
      }
      context_.removeOffer(message.getSourceAddress());
      return null;
    }

    public Void visit(CommPeerFoundMessage message) {
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
            networkQueue_.
                add(new ContractOfferMessage(CryptoUtils.getRandomString(), address, offer));
            context_.addContractOffer(address, offer);
            break;
          }
        }
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
