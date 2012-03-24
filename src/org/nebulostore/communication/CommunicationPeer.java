package org.nebulostore.communication;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.Message;
import org.nebulostore.appcore.Module;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.address.CommAddress;
import org.nebulostore.communication.bdbdht.BdbPeer;
import org.nebulostore.communication.exceptions.CommException;
import org.nebulostore.communication.jxta.JXTAPeer;
import org.nebulostore.communication.kademlia.KademliaPeer;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.nebulostore.communication.messages.bdbdht.BdbMessageWrapper;
import org.nebulostore.communication.messages.dht.DHTMessage;
import org.nebulostore.communication.messages.dht.InDHTMessage;
import org.nebulostore.communication.messages.dht.OutDHTMessage;
import org.nebulostore.communication.messages.kademlia.KademliaMessage;
import org.nebulostore.communication.messages.streambinding.ErrorStreamBindingMessage;
import org.nebulostore.communication.messages.streambinding.StreamBindingMessage;
import org.nebulostore.communication.messages.streambinding.StreamBindingReadyMessage;
import org.nebulostore.communication.streambinding.StreamBindingService;

/**
 * @author Marcin Walas
 */
public class CommunicationPeer extends Module {

  private final JXTAPeer jxtaPeer_;
  private final Module dhtPeer_;

  private final BlockingQueue<Message> jxtaPeerInQueue_;
  private final BlockingQueue<Message> dhtInQueue_;

  private static Logger logger_ = Logger.getLogger(CommunicationPeer.class);
  private static String CONFIGURATION_PATH_ = "resources/conf/communication/CommunicationPeer.xml";

  private static JXTAPeer currJxtaPeer_;

  private final BlockingQueue<Message> streamBindingInQueue_;
  private final StreamBindingService streamBindingService_;

  public CommunicationPeer(BlockingQueue<Message> inQueue,
      BlockingQueue<Message> outQueue) throws NebuloException {
    super(inQueue, outQueue);

    XMLConfiguration config = null;
    try {
      config = new XMLConfiguration(CONFIGURATION_PATH_);
    } catch (ConfigurationException cex) {
      logger_.error("Configuration read error in: " + CONFIGURATION_PATH_);
    }

    jxtaPeerInQueue_ = new LinkedBlockingQueue<Message>();
    dhtInQueue_ = new LinkedBlockingQueue<Message>();

    jxtaPeer_ = new JXTAPeer(jxtaPeerInQueue_, inQueue);
    currJxtaPeer_ = jxtaPeer_;

    streamBindingInQueue_ = new LinkedBlockingQueue<Message>();
    streamBindingService_ = new StreamBindingService(streamBindingInQueue_,
        inQueue, jxtaPeer_.getStreamDriver());
    new Thread(streamBindingService_,
        "Nebulostore.Communication.StreamBindingService").start();

    jxtaPeer_.setStreamBindingService(streamBindingService_);

    new Thread(jxtaPeer_, "Nebulostore.Communication.Jxta").start();

    if (!config.getString("dht.provider", "bdb").equals("none")) {
      if (config.getString("dht.provider", "bdb").equals("bdb")) {
        dhtPeer_ = new BdbPeer(dhtInQueue_, outQueue,
            jxtaPeer_.getPeerDiscoveryService(), getPeerAddress(),
            jxtaPeerInQueue_);
      } else if (config.getString("dht.provider", "bdb").equals("kademlia")) {
        dhtPeer_ = new KademliaPeer(dhtInQueue_, outQueue,
            jxtaPeer_.getPeerDiscoveryService(), getPeerAddress(),
            jxtaPeerInQueue_);
      } else {
        throw new CommException("Unsupported DHT Provider in configuration");
      }
      new Thread(dhtPeer_, "Nebulostore.Communication.DHT").start();
    } else {
      dhtPeer_ = null;
    }
  }

  @Override
  protected void processMessage(Message msg) {
    logger_.debug("Processing message..");

    if (msg instanceof CommPeerFoundMessage) {
      logger_.debug("CommPeerFound message forwarded to Dispatcher");
      outQueue_.add(msg);
      return;
    }

    if (msg instanceof DHTMessage) {
      if (msg instanceof InDHTMessage) {
        logger_.debug("InDHTMessage forwarded to DHT");
        dhtInQueue_.add(msg);
      }
      if (msg instanceof OutDHTMessage) {
        logger_.debug("OutDHTMessage forwarded to Dispatcher");
        outQueue_.add(msg);
      }
      return;
    }

    if (msg instanceof KademliaMessage) {
      logger_.debug("Kademlia DHT message received");
      dhtInQueue_.add(msg);
      return;
    }

    if (msg instanceof StreamBindingMessage) {
      logger_.debug("Stream binding message received");
      streamBindingInQueue_.add(msg);
      return;
    }

    if (msg instanceof StreamBindingReadyMessage) {
      logger_.debug("Stream binding ready message received");
      outQueue_.add(msg);
      return;
    }

    if (msg instanceof ErrorStreamBindingMessage) {
      logger_.debug("Stream binding error message received");
      outQueue_.add(msg);
      return;
    }

    if (msg instanceof BdbMessageWrapper) {
      logger_.debug("BDB DHT message received");
      BdbMessageWrapper casted = (BdbMessageWrapper) msg;
      if (casted.getWrapped() instanceof InDHTMessage) {
        logger_.debug("BDB DHT message forwarded to DHT");
        dhtInQueue_.add(msg);
      }
      if (casted.getWrapped() instanceof OutDHTMessage) {
        logger_.debug("BDB DHT message forwarded to Dispatcher");
        outQueue_.add(casted.getWrapped());
      }
      return;
    }

    if (msg instanceof CommMessage) {
      if (((CommMessage) msg).getDestinationAddress().equals(
          jxtaPeer_.getPeerAddress())) {
        logger_.debug("message forwarded to Dispatcher");
        outQueue_.add(msg);
      } else {
        logger_.debug("message forwarded to jxtaPeer");
        jxtaPeerInQueue_.add(msg);
      }
      return;
    }

  }

  public static CommAddress getPeerAddress() {
    return currJxtaPeer_.getPeerAddress();
  }

  public Module getDHTPeer() {
    return dhtPeer_;
  }
}
