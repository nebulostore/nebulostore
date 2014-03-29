package org.nebulostore.conductor;

import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.async.messages.NetworkContextChangedMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.messages.ErrorMessage;
import org.nebulostore.conductor.messages.FinishMessage;
import org.nebulostore.conductor.messages.GatherStatsMessage;
import org.nebulostore.conductor.messages.StatsMessage;
import org.nebulostore.conductor.messages.TicMessage;
import org.nebulostore.conductor.messages.TocMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Testing module that acts as a Test Server. Remember to set lastPhase_ and peersNeeded_ in
 * subclass.
 *
 * @author szymonmatejczyk
 */
public abstract class ConductorServer extends ReturningJobModule<Boolean> {
  private static final String PHASE_TIMEOUT_MSG = "phase_timeout";
  private static Logger logger_ = Logger.getLogger(ConductorServer.class);
  private static final String N_PEERS_CONFIG = "systest.num-test-participants";

  /**
   * Time(in secs) after which, if not successful, test is failed.
   */
  protected int timeout_;
  protected int phaseTimeout_;

  /**
   * Current phase.
   */
  private int phase_;

  /**
   * Number of Tocs received from peers. After receiving TocMessage from each peer, test advances to
   * next phase.
   */
  private int tocs_;

  /**
   * CommAddresses of the received Tocs.
   */
  Set<CommAddress> tocsAddresses_ = new HashSet<CommAddress>();

  private boolean successful_ = true;

  /**
   * CommAdresses of peers performing this test.
   */
  protected Set<CommAddress> clients_;

  /**
   * JobId that clients use on their side.
   */
  protected final String clientsJobId_;

  /**
   * Last phase of the test. If test moves to next, it is completed successfully.
   */
  protected final int lastPhase_;

  /**
   * Number of peers needed to perform this test.
   */
  protected int peersNeeded_;

  /**
   * Is server used also as one of test clients (false by default).
   */
  protected boolean useServerAsClient_;

  /**
   * Are statistics gathered at the end of test (false by default).
   */
  protected boolean gatherStats_;

  /**
   * Visitor state - either initializing peers or running the tests on constant set of peers.
   */
  enum TestingState {
    COLLECTING_PEERS, INITIALIZING, CONFIGURING, RUNNING, GATHERING_STATS
  };

  private TestingState testingState_;
  private final ServerTestingModuleVisitor visitor_;

  /**
   * Additional, meaningful description of the test. To be printed on test completion.
   */
  private final String testDescription_;

  /**
   * keeps start/stop time of the test.
   */
  private long startTime_ = -1;

  /**
   * Timer for watching maximum stage time.
   */
  private Timer internalCheckTimer_;
  protected CommAddress commAddress_;

  protected NetworkMonitor networkMonitor_;

  protected ConductorServer(int lastPhase, int timeout, String clientsJobId,
                            String testDescription) {
    this(lastPhase, 0, timeout, 0, clientsJobId, testDescription);
  }

  protected ConductorServer(int lastPhase, int peersNeeded, int timeout, String clientsJobId,
      String testDescription) {
    this(lastPhase, peersNeeded, timeout, 0, clientsJobId, testDescription);
  }

  protected ConductorServer(int lastPhase, int peersNeeded, int timeout, int phaseTimeout,
      String clientsJobId, String testDescription) {
    clientsJobId_ = clientsJobId;
    lastPhase_ = lastPhase;
    timeout_ = timeout;
    phaseTimeout_ = phaseTimeout;
    peersNeeded_ = peersNeeded;
    testDescription_ = testDescription;
    visitor_ = new ServerTestingModuleVisitor();
  }

  @Inject
  public void setDependencies(CommAddress commAddress, Timer timer,
      @Named(N_PEERS_CONFIG) int nPeers, NetworkMonitor networkMonitor) {
    commAddress_ = commAddress;
    internalCheckTimer_ = timer;
    networkMonitor_ = networkMonitor;
    if (peersNeeded_ == 0) {
      peersNeeded_ = nPeers;
    }
    if (peersNeeded_ == 0) {
      throw new RuntimeException("Incorrect number of test participants (" + peersNeeded_ + ")");
    }
  }

  protected void schedulePhaseTimer(int phase) {
    if (phaseTimeout_ > 0) {
      internalCheckTimer_.schedule(jobId_, 1000L * phaseTimeout_, PHASE_TIMEOUT_MSG + phase);
    }
  }

  /**
   * Called after enough clients are discovered. Should send messages to discovered peers to
   * initialize test modules on their side.
   */
  public abstract void initClients();

  /**
   * Called after the test is finished. Should fill statistics gathered from test clients.
   */
  public abstract void feedStats(CommAddress sender, CaseStatistics stats);

  /**
   * Called after the test is finished. Should return additional statistics from the test.
   */
  protected abstract String getAdditionalStats();

  protected boolean isSuccessful() {
    return successful_;
  }

  protected void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e1) {
      logger_.debug("Interrupted while sleeping.");
    }
  }

  private boolean trySetClients() {
    clients_ = new HashSet<CommAddress>(networkMonitor_.getKnownPeers());
    if (!useServerAsClient_) {
      clients_.remove(commAddress_);
    }
    if (clients_.size() >= peersNeeded_) {
      logger_.debug("Enough peers in NetworkContext. Initializing clients...");
      return true;
    } else {
      return false;
    }
  }

  // Send FinishMessage to everyone and die.
  private void finishTest() {
    long stopTime;
    logger_.info("Sending finish test messages.");
    internalCheckTimer_.cancelTimer();
    if (clients_ != null) {
      for (CommAddress address : clients_) {
        networkQueue_.add(new FinishMessage(clientsJobId_, null, address));
      }
    }
    stopTime = System.currentTimeMillis();
    if (isSuccessful()) {
      logger_.info("Test " + testDescription_ + " successfull after: " + (stopTime - startTime_) +
          " ms " + getAdditionalStats());
      endWithSuccess(null);
    } else {
      logger_.info("Test " + testDescription_ + " failed. After: " + (stopTime - startTime_) +
          " ms " + getAdditionalStats());
      endWithError(new NebuloException("Unsuccessful test"));
    }
  }

  /**
   * Visitor.
   *
   * @author szymonmatejczyk
   * @author Marcin Walas
   */
  protected class ServerTestingModuleVisitor extends MessageVisitor<Void> {
    private MessageGenerator notificationGenerator_;

    public Void visit(JobInitMessage message) {
      logger_.debug("Received JobInitMessage.");
      jobId_ = message.getId();
      testingState_ = TestingState.COLLECTING_PEERS;
      if (trySetClients()) {
        testingState_ = TestingState.INITIALIZING;
        initClients();
        schedulePhaseTimer(phase_);
      } else {
        /*
         * wait for enough peers to perform the test - start to listen for NetworkContext changes.
         */
        notificationGenerator_ = new MessageGenerator() {
          @Override
          public Message generate() {
            return new NetworkContextChangedMessage(getJobId());
          }
        };
        networkMonitor_.addContextChangeMessageGenerator(notificationGenerator_);
        logger_.debug("Waiting for peer discovery.");
      }
      return null;
    }

    public Void visit(NetworkContextChangedMessage message) {
      logger_.debug("Received NetworkContextChangedMessage (we know " +
          networkMonitor_.getKnownPeers().size() + " peers).");
      if (testingState_ == TestingState.COLLECTING_PEERS && trySetClients()) {
        testingState_ = TestingState.INITIALIZING;
        initClients();
        schedulePhaseTimer(phase_);
      }
      return null;
    }

    public Void visit(TocMessage message) {
      if (!clients_.contains(message.getSourceAddress()) || message.getPhase() != phase_) {
        return null;
      }

      logger_.debug("TocMessage received from: " + message.getSourceAddress() + ". Tocs: " + tocs_);

      if (tocsAddresses_.contains(message.getSourceAddress())) {
        logger_.debug("Already received toc from this address");
        return null;
      }

      synchronized (tocsAddresses_) {
        tocs_++;
        tocsAddresses_.add(message.getSourceAddress());
        logger_.debug("Tocs incremented to: " + tocs_);

        if (tocs_ < peersNeeded_) {
          Set<CommAddress> tmp = new HashSet<CommAddress>(clients_);
          tmp.removeAll(tocsAddresses_);
          logger_.debug("Still waiting for " + (peersNeeded_ - tocs_) + " peers from: "  +
              tmp.toString());
        }
        processTocsChange();
      }
      return null;
    }

    public Void visit(StatsMessage message) {
      if (!clients_.contains(message.getSourceAddress())) {
        logger_.warn("Received StatsMessage with no source address.");
        return null;
      }
      tocs_++;
      logger_.debug("Got stats message. (" + tocs_ + "/" + peersNeeded_ + ") from " +
          message.getSourceAddress());

      feedStats(message.getSourceAddress(), message.getStats());
      if (tocs_ >= peersNeeded_) {
        finishTest();
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      if ((testingState_ == TestingState.INITIALIZING) &&
          (PHASE_TIMEOUT_MSG + phase_).equals(message.getMessageContent())) {
        logger_.warn("Phase timeout in initializing phase. " + tocs_ + " tocs out of " +
          peersNeeded_ + " received");
        tocs_ = peersNeeded_;
        processTocsChange();
      } else if ((testingState_ == TestingState.RUNNING) &&
          (PHASE_TIMEOUT_MSG + phase_).equals(message.getMessageContent())) {
        logger_.warn("Phase timeout in phase " + phase_ + ". " + tocs_ + " tocs out of " +
          peersNeeded_ + " received");
        tocs_ = peersNeeded_;
        processTocsChange();
      } else if ((testingState_ == TestingState.GATHERING_STATS) &&
          (PHASE_TIMEOUT_MSG + (lastPhase_ + 1)).equals(message.getMessageContent())) {
        logger_.warn("Phase timeout in gathering stats phase. " + tocs_ + " tocs out of " +
          peersNeeded_ + " received");
        finishTest();
      }
      return null;
    }

    public Void visit(ErrorMessage message) {
      logger_.warn("Received ErrorMessage, test failed: " + message.getMessage());
      successful_ = false;
      finishTest();
      return null;
    }
  }

  private void advancePhase() {
    ++phase_;
    tocs_ = 0;
    tocsAddresses_ = new HashSet<CommAddress>();
    sendTics();
    schedulePhaseTimer(phase_);
  }

  private void processTocsChange() {
    if (testingState_ == TestingState.INITIALIZING) {
      logger_.debug("In state initializing");
      if (tocs_ >= peersNeeded_) {
        clients_ = tocsAddresses_;
        logger_.debug("clients set modified to: " + clients_);
        logger_.info("Advancing to phase: " + (phase_ + 1));
        startTime_ = System.currentTimeMillis();
        advancePhase();
        testingState_ = TestingState.RUNNING;
      }
    } else if (testingState_ == TestingState.RUNNING) {
      logger_.debug("In state running phase: " + phase_);
      if (tocs_ >= peersNeeded_) {
        if ((phase_ + 1) <= lastPhase_) {
          logger_.info("Advancing to phase: " + (phase_ + 1));
          advancePhase();
        } else {
          ++phase_;
          if (gatherStats_) {
            logger_.info("Advancing to phase: " + phase_ + " (gathering stats)");
            testingState_ = TestingState.GATHERING_STATS;
            tocs_ = 0;
            sendGatherStats();
          } else {
            finishTest();
          }
        }
      }
    }
  }

  private void sendTics() {
    logger_.debug("Sending Tic messages...");
    for (CommAddress address : clients_) {
      networkQueue_.add(new TicMessage(clientsJobId_, null, address, phase_));
    }
  }

  private void sendGatherStats() {
    logger_.debug("Getting stats from test clients...");
    for (CommAddress address : clients_) {
      networkQueue_.add(new GatherStatsMessage(clientsJobId_, null, address));
    }
    schedulePhaseTimer(lastPhase_ + 1);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  public Boolean getResult() throws NebuloException {
    return getResult(timeout_);
  }
}
