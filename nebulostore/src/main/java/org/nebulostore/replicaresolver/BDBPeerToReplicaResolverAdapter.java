package org.nebulostore.replicaresolver;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

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
  private final ReplicaResolver contractMap_;
  private final MessageVisitor<Void> msgVisitor_;

  public BDBPeerToReplicaResolverAdapter(
    BlockingQueue<Message> inQueue,
    BlockingQueue<Message> outQueue,
    ReplicaResolver replicaResolver) {
    super(inQueue, outQueue);
    contractMap_ = replicaResolver;
    msgVisitor_ = new BDBServerMessageVisitor();
  }

  @Override
  protected void processMessage(Message msg) throws NebuloException {
    LOGGER.debug(String.format("processMessage(%s)", msg));
    msg.accept(msgVisitor_);
  }

  private void get(GetDHTMessage getMsg) {
    LOGGER.debug(String.format("get(%s)", getMsg));
    KeyDHT key = getMsg.getKey();
    ValueDHT value = null;
    try {
      value = contractMap_.get(key);
    } catch (IOException e) {
      LOGGER.warn("get() -> ERROR", e);
    }

    OutDHTMessage outMessage;
    if (value != null) {
      outMessage = new ValueDHTMessage(getMsg, key, value);
    } else {
      outMessage = new ErrorDHTMessage(getMsg, new NebuloException(
          "Unable to read from database."));
    }

    outQueue_.add(outMessage);
  }

  private void put(PutDHTMessage putMsg) {
    KeyDHT key = putMsg.getKey();
    ValueDHT value = putMsg.getValue();

    try {
      contractMap_.put(key, value);
      outQueue_.add(new OkDHTMessage(putMsg));
    } catch (IOException e) {
      LOGGER.error(String.format("put(%s)", putMsg), e);
    }
  }

  /**
   * Message Visitor for server BDB Peer.
   *
   * @author Grzegorz Milka
   */
  protected final class BDBServerMessageVisitor extends MessageVisitor<Void> {

    public Void visit(EndModuleMessage msg) {
      endModule();
      return null;
    }

    public Void visit(GetDHTMessage msg) {
      get(msg);
      return null;
    }

    public Void visit(PutDHTMessage msg) {
      put(msg);
      return null;
    }
  }

}

