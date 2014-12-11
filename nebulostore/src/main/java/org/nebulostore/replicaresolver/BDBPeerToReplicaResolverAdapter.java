package org.nebulostore.replicaresolver;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.Module;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.OutDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;

public class BDBPeerToReplicaResolverAdapter extends Module {
  private static final Logger LOGGER = Logger.getLogger(BDBPeerToReplicaResolverAdapter.class);

  private static final long EXECUTOR_SHUTDOWN_TIMEOUT_SEC = 10;

  private final ReplicaResolver contractMap_;
  private final MessageVisitor<Void> msgVisitor_;
  private final ExecutorService executor_;

  @Inject
  public BDBPeerToReplicaResolverAdapter(
      @Assisted("communication.dht.inQueue") BlockingQueue<Message> inQueue,
      @Assisted("communication.dht.outQueue") BlockingQueue<Message> outQueue,
      @Assisted ReplicaResolver replicaResolver,
      @Named("communication.dht.executor") ExecutorService executor) {
    super(inQueue, outQueue);
    contractMap_ = replicaResolver;
    executor_ = executor;
    msgVisitor_ = new BDBServerMessageVisitor();
  }

  @Override
  protected void processMessage(Message msg) throws NebuloException {
    LOGGER.debug(String.format("processMessage(%s)", msg));
    msg.accept(msgVisitor_);
  }

  /**
   * Message Visitor for server BDB Peer.
   *
   * @author Grzegorz Milka
   */
  protected final class BDBServerMessageVisitor extends MessageVisitor<Void> {

    public Void visit(EndModuleMessage msg) {
      executor_.shutdown();
      try {
        executor_.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SEC, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Error while waiting for executor service termination.");
        throw new IllegalStateException(e);
      }
      endModule();
      return null;
    }

    public Void visit(GetDHTMessage msg) {
      executor_.submit(new GetDHTRunnable(msg));
      return null;
    }

    public Void visit(PutDHTMessage msg) {
      executor_.submit(new PutDHTRunnable(msg));
      return null;
    }
  }

  private class GetDHTRunnable implements Runnable {
    private final GetDHTMessage getMsg_;

    public GetDHTRunnable(GetDHTMessage getMsg) {
      getMsg_ = getMsg;
    }

    @Override
    public void run() {
      LOGGER.debug(String.format("get(%s)", getMsg_));
      KeyDHT key = getMsg_.getKey();
      ValueDHT value = null;
      try {
        value = contractMap_.get(key);
      } catch (IOException e) {
        LOGGER.warn("get() -> ERROR", e);
      }

      OutDHTMessage outMessage;
      if (value != null) {
        outMessage = new ValueDHTMessage(getMsg_, key, value);
      } else {
        outMessage =
            new ErrorDHTMessage(getMsg_, new NebuloException("Unable to read from database."));
      }

      outQueue_.add(outMessage);
    }
  }

  private class PutDHTRunnable implements Runnable {
    private final PutDHTMessage putMsg_;

    public PutDHTRunnable(PutDHTMessage putMsg) {
      putMsg_ = putMsg;
    }

    @Override
    public void run() {
      KeyDHT key = putMsg_.getKey();
      ValueDHT value = putMsg_.getValue();

      try {
        contractMap_.put(key, value);
        outQueue_.add(new OkDHTMessage(putMsg_));
      } catch (IOException e) {
        LOGGER.error(String.format("put(%s)", putMsg_), e);
      }
    }
  }

}
