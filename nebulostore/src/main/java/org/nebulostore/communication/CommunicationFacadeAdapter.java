package org.nebulostore.communication;

import java.io.IOException;
import java.util.Collection;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.Module;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.CommPeerFoundMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.AddressNotPresentException;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.routing.MessageListener;
import org.nebulostore.communication.routing.MessageMatcher;
import org.nebulostore.communication.routing.SendResult;
import org.nebulostore.dht.messages.DHTMessage;
import org.nebulostore.dht.messages.InDHTMessage;
import org.nebulostore.dht.messages.OutDHTMessage;
import org.nebulostore.dht.messages.ReconfigureDHTMessage;
import org.nebulostore.replicaresolver.DHTPeerFactory;
import org.nebulostore.replicaresolver.ReplicaResolver;
import org.nebulostore.replicaresolver.ReplicaResolverFactory;

/**
 * @author Grzegorz Milka
 */
public class CommunicationFacadeAdapter extends Module {
  private static final Logger LOGGER = Logger.getLogger(CommunicationFacadeAdapter.class);
  private final CommunicationFacade commFacade_;
  private final PeerFoundObserver peerFoundObserver_;
  private final MessageVisitor<Void> msgVisitor_;

  private final AtomicBoolean isEnding_;

  private final BlockingQueue<SendResult> sendResults_;

  private final MsgSendMonitor msgSendMonitor_;
  private Future<?> msgSendMonitorFuture_;

  private final ExecutorService executor_;

  private final CommAddress localCommAddress_;

  private final ReplicaResolverFactory contractMapFactory_;
  private ReplicaResolver contractMap_;

  private final CommMessageListener msgListener_ = new CommMessageListener();
  private final CommMessageMatcher msgMatcher_ = new CommMessageMatcher();

  // TODO change bdb
  /**
   * DHT module available to higher layers.
   *
   * Note that it was implemented by Marcin and I(grzegorzmilka) left it mostly as is. Only BDB
   * works.
   */
  private final DHTPeerFactory dhtPeerFactory_;
  private Module dhtPeer_;
  private final BlockingQueue<Message> dhtPeerInQueue_;
  private Thread dhtPeerThread_;

  @AssistedInject
  public CommunicationFacadeAdapter(
      @Assisted("CommunicationPeerInQueue") BlockingQueue<Message> inQueue,
      @Assisted("CommunicationPeerOutQueue") BlockingQueue<Message> outQueue,
      CommunicationFacade commFacade,
      @Named("communication.local-comm-address") CommAddress localCommAddress,
      @Named("communication.main-executor") ExecutorService executor,
      ReplicaResolverFactory replicaResolverFactory,
      DHTPeerFactory dhtPeerFactory) {

    super(inQueue, outQueue);
    commFacade_ = commFacade;
    peerFoundObserver_ = new PeerFoundObserver();
    msgVisitor_ = new CommFacadeAdapterMsgVisitor();

    isEnding_ = new AtomicBoolean(false);
    sendResults_ = new LinkedBlockingQueue<>();

    msgSendMonitor_ = new MsgSendMonitor();
    localCommAddress_ = localCommAddress;
    contractMapFactory_ = replicaResolverFactory;
    dhtPeerFactory_ = dhtPeerFactory;
    executor_ = executor;

    dhtPeerInQueue_ = new LinkedBlockingQueue<Message>();
  }

  @Override
  protected void initModule() {
    try {
      msgSendMonitorFuture_ = executor_.submit(msgSendMonitor_);
      commFacade_.addMessageListener(msgMatcher_, msgListener_);
      commFacade_.startUp();
      commFacade_.addPeerFoundListener(peerFoundObserver_);
      contractMapFactory_.startUp();
      contractMap_ = contractMapFactory_.getContractMap();

      startUpReplicaResolver();
    } catch (IOException e) {
      throw new RuntimeException("Unable to initialize network!", e);
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    LOGGER.debug(String.format("processMessage(%s)", message));
    message.accept(msgVisitor_);
  }

  private void shutDown() {
    try {
      executor_.shutdown();
      shutDownReplicaResolver();
      contractMapFactory_.shutDown();
      commFacade_.removePeerFoundListener(peerFoundObserver_);
      commFacade_.shutDown();
      commFacade_.removeMessageListener(msgListener_);
      msgSendMonitorFuture_.cancel(true);
    } catch (InterruptedException e) {
      throw new IllegalStateException();
    }
    endModule();
    LOGGER.trace("shutdown(): void");
  }

  /**
   * Starts up replica resolver.
   *
   * @author Marcin Walas
   * @author Grzegorz Milka
   */
  private void startUpReplicaResolver() {
    LOGGER.trace("startUpReplicaResolver()");
    dhtPeer_ = dhtPeerFactory_.createDHTPeer(dhtPeerInQueue_, inQueue_, contractMap_);
    dhtPeerThread_ = new Thread(dhtPeer_, "Nebulostore.Communication.DHT");
    dhtPeerThread_.start();
  }

  private void shutDownReplicaResolver() throws InterruptedException {
    dhtPeerInQueue_.add(new EndModuleMessage());
    dhtPeerThread_.join();
  }

  /**
   * Message Visitor for {@link CommunicationFacadeAdapter}.
   *
   * @author Grzegorz Milka
   */
  protected final class CommFacadeAdapterMsgVisitor extends MessageVisitor<Void> {
    public Void visit(EndModuleMessage msg) {
      isEnding_.set(true);
      shutDown();
      return null;
    }

    public Void visit(ReconfigureDHTMessage msg) {
      LOGGER.warn("Got reconfigure request with jobId: " + msg.getId());
      /*
       * reconfigureDHT(((ReconfigureDHTMessage) msg).getProvider(), (ReconfigureDHTMessage) msg);
       */
      return null;
    }

    public Void visit(DHTMessage msg) {
      if (msg instanceof InDHTMessage) {
        LOGGER.debug("InDHTMessage forwarded to DHT" + msg.getClass().getSimpleName());
        dhtPeerInQueue_.add(msg);
      } else if (msg instanceof OutDHTMessage) {
        LOGGER.debug("OutDHTMessage forwarded to Dispatcher" + msg.getClass().getSimpleName());
        outQueue_.add(msg);
      } else {
        LOGGER.warn("Unrecognized DHTMessage: " + msg);
      }
      return null;
    }

    public Void visit(CommMessage msg) {
      if (msg.getDestinationAddress() == null) {
        LOGGER.warn("Null destination address set for " + msg + ". Dropping the message.");
      } else {
        commFacade_.sendMessage(msg, sendResults_);
      }
      return null;
    }
  }

  private class CommMessageListener implements MessageListener {
    @Override
    public void onMessageReceive(Message msg) {
      outQueue_.add(msg);
    }
  }

  private class CommMessageMatcher implements MessageMatcher {
    @Override
    public boolean matchMessage(CommMessage msg) {
      return true;
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private class MsgSendMonitor implements Runnable {
    @Override
    public void run() {
      while (true) {
        SendResult result;
        try {
          result = sendResults_.take();
        } catch (InterruptedException e) {
          break;
        }
        try {
          result.getResult();
        } catch (AddressNotPresentException | IOException e) {
          LOGGER.warn(String.format("sendMessage(%s) -> error", result.getMsg()), e);
          outQueue_.add(new ErrorCommMessage(result.getMsg(), e));
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  /**
   * @author Grzegorz Milka
   */
  private class PeerFoundObserver implements Observer {
    @Override
    public void update(Observable arg0, Object arg1) {
      @SuppressWarnings("unchecked")
      Collection<CommAddress> newPeers = (Collection<CommAddress>) arg1;
      for (CommAddress newPeer : newPeers) {
        outQueue_.add(new CommPeerFoundMessage(newPeer, localCommAddress_));
      }
    }
  }
}
