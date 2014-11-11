package org.nebulostore.api;

import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.Metadata;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ReplicationGroup;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.ListAppender;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.appcore.model.NebuloList;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.messages.ErrorCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.AppendElementsMessage;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.ReplicatorMessage;


public class AppendToNebuloListModule extends ReturningJobModule<Void> implements ListAppender {
  private static Logger logger_ = Logger.getLogger(AppendToNebuloListModule.class);

  private NebuloList list_;
  private List<NebuloElement> elementsToAppend_;

  private final StateMachineVisitor visitor_ = new StateMachineVisitor();

  @Override
  public void appendElements(NebuloList list, List<NebuloElement> elementsToAppend) {
    list_ = list;
    elementsToAppend_ = elementsToAppend;
    runThroughDispatcher();
  }

  @Override
  public void awaitResult(int timeoutSec) throws NebuloException {
    getResult(timeoutSec);
  };

  /**
   * States of the state machine.
   */
  private enum STATE {
    INIT, DHT_QUERY, ELEMENTS_APPEND
  }

  /**
   * Visitor class that acts as a state machine realizing the procedure of
   * appending a list of NebuloElements to NebuloList by contacting with one of Replicators.
   */
  protected class StateMachineVisitor extends MessageVisitor<Void> {
    private STATE state_;
    private CommAddress recipient_;

    public StateMachineVisitor() {
      state_ = STATE.INIT;
    }

    public Void visit(JobInitMessage message) {
      if (state_ == STATE.INIT) {
        NebuloAddress address = list_.getAddress();
        logger_.debug("Quering DHT for replicators for NebuloAddress = " + address);
        state_ = STATE.DHT_QUERY;
        logger_.debug(
            "Adding GetDHT to network queue (" + address.getAppKey() + ", " + jobId_ + ").");
        networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(address.getAppKey().getKey())));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      logger_.debug("Got ValueDHTMessage " + message.toString());
      if (state_ == STATE.DHT_QUERY) {
        state_ = STATE.ELEMENTS_APPEND;

        Metadata metadata = (Metadata) message.getValue().getValue();
        logger_.debug("Metadata: " + metadata);

        ContractList contractList = metadata.getContractList();
        logger_.debug("ContractList: " + contractList);

        ReplicationGroup group = contractList.getGroup(list_.getObjectId());
        logger_.debug("Group: " + group);

        if (group == null || group.getSize() == 0) {
          endWithError(new NebuloException("No peers replicating this object."));
        } else {
          recipient_ = chooseReplicator(group);
          networkQueue_.add(new AppendElementsMessage(
              recipient_, list_.getObjectId(), elementsToAppend_, group, getJobId()));
        }
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    private CommAddress chooseReplicator(ReplicationGroup replicators) {
      return replicators.iterator().next();
    }

    public Void visit(ErrorDHTMessage message) {
      if (state_ == STATE.DHT_QUERY) {
        logger_.debug("Received ErrorDHTMessage");
        endWithError(
            new NebuloException("Could not fetch metadata from DHT.", message.getException()));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    public Void visit(ConfirmationMessage message) {
      if (state_ == STATE.ELEMENTS_APPEND && message.getSourceAddress().equals(recipient_)) {
        logger_.debug("Received confirmation for append() operation.");
        endWithSuccess(null);
      } else {
         incorrectState(state_.name(), message);
      }
      return null;
    }

    // Should we retry with another replicator?
    public Void visit(ErrorCommMessage message) {
      if (state_ == STATE.ELEMENTS_APPEND) {
        logger_.debug("Received ErrorCommMessage for append() opearation.");
        endWithError(new NebuloException(
            "Contacted replicator refused append(). Error information: " + message.getMessage()));
      } else {
        incorrectState(state_.name(), message);
      }
      return null;
    }

    private void incorrectState(String stateName, Message message) {
      String messageName = message.getClass().getSimpleName();
      logger_.warn(messageName + " received in state " + stateName);
    }

    private void incorrectState(String stateName, ReplicatorMessage message) {
      String messageName = message.getClass().getSimpleName();
      String sender = message.getSourceAddress().toString();
      logger_.warn(messageName + " from " + sender + " received in state " + stateName);
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
