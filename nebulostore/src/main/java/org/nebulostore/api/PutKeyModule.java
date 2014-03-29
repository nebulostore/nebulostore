package org.nebulostore.api;

import java.util.concurrent.BlockingQueue;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.IntervalCollisionException;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Job module that realizes putKey() API function.
 * @author Bolek Kulbabinski
 */
public class PutKeyModule extends ReturningJobModule<Void> {
  private AppKey appKey_;
  private final StateMachineVisitor visitor_;
  private ReplicationGroup replicationGroup_;

  private static Logger logger_ = Logger.getLogger(PutKeyModule.class);

  @Inject
  public void setAppKey(AppKey appKey) {
    appKey_ = appKey;
  }

  /**
   * Constructor that runs newly created module.
   */
  public PutKeyModule(ReplicationGroup replicationGroup, BlockingQueue<Message> dispatcherQueue) {
    visitor_ = new StateMachineVisitor();
    replicationGroup_ = replicationGroup;
    setDispatcherQueue(checkNotNull(dispatcherQueue));
    runThroughDispatcher();
  }

  /**
   * States of the state machine.
   */
  private enum STATE { INIT, DHT_INSERT };

  /**
   * Visitor class that acts as a state machine realizing the procedure of creating new top-level
   * directory for a user.
   */
  protected class StateMachineVisitor extends MessageVisitor<Void> {
    private STATE state_;

    public StateMachineVisitor() {
      state_ = STATE.INIT;
    }

    public Void visit(JobInitMessage message) {
      checkNotNull(appKey_);
      if (state_ == STATE.INIT) {
        // State 1 - Send appKey to DHT and wait for reply.
        state_ = STATE.DHT_INSERT;
        jobId_ = message.getId();

        // List of top-dir replicators stored in DHT.
        ContractList contractList = new ContractList();
        try {
          // TODO(bolek, marcin): This will be replaced with a DHT update function.
          contractList.addGroup(replicationGroup_);
        } catch (IntervalCollisionException exception) {
          endWithError(new NebuloException("Error while creating replication group", exception));
        }
        logger_.debug("Updating top dir in DHT, contracts list: " + contractList.toString());
        ValueDHT value = new ValueDHT(new Metadata(appKey_, contractList));
        KeyDHT key = new KeyDHT(appKey_.getKey());
        networkQueue_.add(new PutDHTMessage(jobId_, key, value));
      } else {
        logger_.warn("JobInitMessage received in state " + state_);
      }
      return null;
    }

    public Void visit(OkDHTMessage message) {
      if (state_ == STATE.DHT_INSERT) {
        logger_.debug("Successfully updated top dir in DHT");
        endWithSuccess(null);
      } else {
        logger_.warn("OkDHTMessage received in state " + state_);
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      if (state_ == STATE.DHT_INSERT) {
        endWithError(new NebuloException("DHT write returned with error", message.getException()));
      } else {
        logger_.warn("ErrorDHTMessage received in state " + state_);
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
