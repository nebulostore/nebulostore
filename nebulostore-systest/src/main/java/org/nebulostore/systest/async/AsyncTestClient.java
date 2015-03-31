package org.nebulostore.systest.async;

import java.util.List;

import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.async.synchrogroup.ChangeSynchroPeerSetModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.GatherStatsMessage;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.StatsMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.systest.async.messages.CounterValueMessage;
import org.nebulostore.systest.async.messages.DisableCommunicationMessage;
import org.nebulostore.systest.async.messages.EnableCommunicationMessage;
import org.nebulostore.systest.async.messages.GetCounterValueMessage;
import org.nebulostore.systest.async.messages.IncrementMessage;


/**
 * Client for asynchronous messages test.
 *
 * @author Piotr Malicki
 */
public class AsyncTestClient extends ConductorClient {
  private static final long serialVersionUID = -7209499692156491320L;
  private static Logger logger_ = Logger.getLogger(AsyncTestClient.class);
  /**
   * Sleep time while waiting for asynchronous messages synchronization.
   */
  private static final int SLEEP_TIME = 20000;

  private final List<CommAddress> myReceivers_;
  private final List<CommAddress> mySynchroPeers_;
  private final AsyncTestClientState state_;
  private CommAddress myAddress_;
  private transient CounterModule counterModule_;

  @Inject
  public void setDependencies(CommAddress myAddress, CounterModule counterModule) {
    myAddress_ = myAddress;
    counterModule_ = counterModule;
  }

  public AsyncTestClient(String serverJobId, int numPhases, CommAddress serverCommAddress,
      List<CommAddress> myReceivers, List<CommAddress> mySynchroPeers, AsyncTestClientState state) {
    super(serverJobId, numPhases, serverCommAddress);
    myReceivers_ = myReceivers;
    mySynchroPeers_ = mySynchroPeers;
    state_ = state;
    logger_.debug("Creating new AsyncTestClient:\n" + "My comm-address: " + myAddress_ + "\n" +
        "My synchro-peer: " + mySynchroPeers + "\n" + "My receivers: " + myReceivers + "\n");
  }

  @Override
  protected void initVisitors() {
    visitors_ = new TestingModuleVisitor[numPhases_ + 3];
    visitors_[0] = new EmptyInitializationVisitor();
    visitors_[1] = new SynchroPeerInitializationVisitor();
    AsyncTestClientState tmpState = state_;
    for (int i = 2; i < numPhases_ + 2; i += 3) {
      switch (tmpState) {
        case INACTIVE :
          visitors_[i] = new DisableCommunicationVisitor();
          visitors_[i + 1] = new EmptyPhaseVisitor();
          visitors_[i + 2] = new EnableCommunicationVisitor();
          tmpState = AsyncTestClientState.SENDER;
          break;
        case SENDER :
          visitors_[i] = new EmptyPhaseVisitor();
          visitors_[i + 1] = new SendMessagesVisitor();
          visitors_[i + 2] = new EmptyPhaseVisitor();
          tmpState = AsyncTestClientState.SYNCHRO_PEER;
          break;
        case SYNCHRO_PEER :
          visitors_[i] = new EmptyPhaseVisitor();
          visitors_[i + 1] = new EmptyPhaseVisitor();
          visitors_[i + 2] = new EmptyPhaseVisitor();
          tmpState = AsyncTestClientState.INACTIVE;
          break;
        default :
          break;
      }
    }
    visitors_[numPhases_ + 2] = new AsyncTestLastPhaseVisitor();
  }

  protected class EmptyPhaseVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.info("Starting empty phase of test.");
      phaseFinished();
    }

  }

  protected class SynchroPeerInitializationVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.info("Starting initialization phase of test. Adding peer with address " +
          mySynchroPeers_ + "as a synchro-peer of peer with address " + myAddress_);
      outQueue_.add(new JobInitMessage(new ChangeSynchroPeerSetModule(Sets
          .newHashSet(mySynchroPeers_), null)));
      sleep(SLEEP_TIME);
      phaseFinished();
    }

  }

  protected class DisableCommunicationVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.info("Starting disable communication phase");
      networkQueue_.add(new DisableCommunicationMessage());
      phaseFinished();
    }
  }

  protected class EnableCommunicationVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.info("Starting enable communication phase");
      networkQueue_.add(new EnableCommunicationMessage());
      sleep(SLEEP_TIME);
      phaseFinished();
    }

  }

  protected class SendMessagesVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.info("Starting send messages phase");
      for (int i = 0; i < myReceivers_.size(); i++) {
        IncrementMessage msg = new IncrementMessage(myAddress_, myReceivers_.get(i));
        networkQueue_.add(msg);
      }
      sleep(SLEEP_TIME);
      phaseFinished();
    }

  }

  protected class AsyncTestLastPhaseVisitor extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
    }

    public void visit(CounterValueMessage message) {
      networkQueue_.add(new StatsMessage(serverJobId_, myAddress_, server_,
          new AsyncTestStatistics(message.getValue())));
    }

    public void visit(GatherStatsMessage message) {
      logger_.debug("Received GatherStatsMessage");
      counterModule_.getInQueue().add(new GetCounterValueMessage(inQueue_, jobId_));
    }

  }

  public static enum AsyncTestClientState {
    SYNCHRO_PEER, SENDER, INACTIVE
  }
}
