package org.nebulostore.conductor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.messages.ErrorMessage;
import org.nebulostore.conductor.messages.FinishMessage;
import org.nebulostore.conductor.messages.GatherStatsMessage;
import org.nebulostore.conductor.messages.InitMessage;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.StatsMessage;
import org.nebulostore.conductor.messages.TicMessage;
import org.nebulostore.conductor.messages.TocMessage;
import org.nebulostore.conductor.messages.UserCommMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Base class for all TestingModules(test cases run on peers).
 *
 * @author szymonmatejczyk
 *
 * Writing tests:
 *
 *   1. Remember to set visitors for each phase.
 *   2. Don't forget to call phaseFinished() in every visitor
 *   3. By default you should define visitor for each phase. However, you can override
 *      getVisitor() method to use visitors differently(ex. more than once).
 *   4. Don't forget to write server(ServerTestingModule) that will initialize
 *      TestingModules on peers side and will be gathering results.
 *   5. Don't send any messages in a new phase until you received TicMessage from the server.
 *   6. When you receive TicMessage(n+1), you can assume that every peer have finished n-th phase.
 */
public abstract class ConductorClient extends JobModule implements Serializable {
  private static final long serialVersionUID = -1686614265302231592L;
  private static Logger logger_ = Logger.getLogger(ConductorClient.class);

  protected final CommAddress server_;
  protected final String serverJobId_;
  protected final int numPhases_;

  protected Provider<Timer> timers_;
  protected int phase_;

  private boolean canSendMessages_;
  private final Queue<Message> waitingMessages_ = new LinkedList<Message>();

  public ConductorClient(String serverJobId, int numPhases, CommAddress serverCommAddress) {
    serverJobId_ = serverJobId;
    numPhases_ = numPhases;
    server_ = serverCommAddress;
  }

  @Inject
  public void setTimerProvider(Provider<Timer> timers) {
    timers_ = timers;
  }

  /**
   * Called after receiving Tic Message, which means that all peer have finished previous phase.
   */
  protected void advancedToNextPhase() {
    canSendMessages_ = true;
    inQueue_.add(new NewPhaseMessage());
  }

  private void sendWaitingMessages() {
    logger_.debug(String.format("Send %d waiting messages", waitingMessages_.size()));
    while (!waitingMessages_.isEmpty()) {
      Message message = waitingMessages_.poll();
      networkQueue_.add(message);
      if (message instanceof TocMessage) {
        ++phase_;
      }
    }
  }

  protected boolean sendMessage(Message message) {
    if (canSendMessages_) {
      networkQueue_.add(message);
      return true;
    } else {
      logger_.debug(String.format("Wait for send message: %s", message.toString()));
      waitingMessages_.add(message);
      return false;
    }
  }

  protected void phaseFinished() {
    logger_.debug("Phase finished. Sending TocMessage");
    sendMessage(new TocMessage(serverJobId_, null, server_, phase_));
    if (waitingMessages_.isEmpty()) {
      ++phase_;
      canSendMessages_ = false;
    }
  }

  protected void endWithError(String message) {
    logger_.error(message);
    networkQueue_.add(new ErrorMessage(serverJobId_, null, server_, message));
    super.endModule();
  }

  /**
   * Visitors for phases. They are never send but initialized on clients side.
   */
  protected transient TestingModuleVisitor[] visitors_;

  /**
   * Get visitor for current phase. Null if bad phase (including one after the last, meaning the
   * test has ended for that peer).
   */
  private TestingModuleVisitor getVisitor() {
    if (visitors_ == null) {
      initVisitors();
    }
    if (visitors_ != null && phase_ < visitors_.length) {
      return visitors_[phase_];
    } else {
      return null;
    }
  }

  protected abstract void initVisitors();

  protected void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e1) {
      logger_.debug("Interrupted while sleeping.");
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    TestingModuleVisitor visitor = getVisitor();
    if (visitor != null) {
      message.accept(visitor);
    } else {
      logger_.debug("ignoring " + message.getClass().getSimpleName() + " in last phase.");
    }
  }

  protected void assertTrue(Boolean b, String message) {
    if (!b) {
      logger_.warn("Assertion failed: " + message);
      networkQueue_.add(new ErrorMessage(serverJobId_, null, server_, message));
    }
  }

  /**
   * Visitor handling Tic and FinishTest messages.
   *
   * @author szymonmatejczyk
   */
  protected abstract class TestingModuleVisitor extends MessageVisitor {
    public abstract void visit(NewPhaseMessage message);

    public void visit(TicMessage message) {
      logger_.debug("TicMessage received. Current phase: " + phase_ + "; Server phase: " +
          message.getPhase());
      if (message.getPhase() == phase_) {
        logger_.debug("TicMessage - executing phase " + phase_);
        if (!waitingMessages_.isEmpty()) {
          sendWaitingMessages();
        } else {
          advancedToNextPhase();
        }
      }
    }

    public void visit(UserCommMessage message) {
      if (message.getPhase() != phase_) {
        logger_.warn("Received UserCommMessage from phase " + message.getPhase() +
            " while in phase " + phase_);
      } else {
        visitCorrectPhaseUserCommMessage(message);
      }
    }

    protected void visitCorrectPhaseUserCommMessage(UserCommMessage message) {
      // Override in subclass.
    }

    public void visit(TimeoutMessage message) {
      logger_.warn("Received TimeoutMessage from phase " + message.getMessageContent() +
          " while in phase " + phase_);
    }

    public void visit(FinishMessage message) {
      logger_.info("Test finished by server.");
      endJobModule();
    }
  }

  /**
   * Empty visitor for phase 0.
   *
   * @author szymonmatejczyk
   */
  protected class EmptyInitializationVisitor extends TestingModuleVisitor {
    public EmptyInitializationVisitor() {
    }

    public Void visit(InitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Test client initialized: " + message.getHandler().getClass().getSimpleName());
      canSendMessages_ = true;
      phaseFinished();
      return null;
    }

    @Override
    public void visit(NewPhaseMessage message) {
    }
  }

  /**
   * Empty visitor.
   *
   * @author lukaszsiczek
   */
  protected class EmptyVisitor extends TestingModuleVisitor {
    public EmptyVisitor() {
    }

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.debug("Ignore Phase no: " + phase_);
      phaseFinished();
    }
  }

  /**
   * Visitor that waits @param timeout_ to finish phase.
   */
  public class DelayingVisitor extends TestingModuleVisitor {
    protected final long timeout_;
    protected final Timer timer_;

    public DelayingVisitor(long timeout, Timer timer) {
      timeout_ = timeout;
      timer_ = timer;
    }

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.debug("Phase delaying started... (" + timeout_ + " ms)");
      timer_.schedule(jobId_, timeout_);
    }

    @Override
    public void visit(TimeoutMessage message) {
      logger_.debug("Phase delaying finished.");
      phaseFinished();
    }
  }

  /**
   * Visitor that ignores NewPhaseMessage.
   *
   * @author szymonmatejczyk
   */
  protected class IgnoreNewPhaseVisitor extends TestingModuleVisitor {
    public IgnoreNewPhaseVisitor() {
    }

    @Override
    public void visit(NewPhaseMessage message) {
    }
  }

  /**
   * Default visitor for last phase. Handles GatherStatsMessage (and FinishMessage).
   *
   * @author Bolek Kulbabinski
   */
  protected class LastPhaseVisitor extends TestingModuleVisitor {
    protected CaseStatistics stats_;

    public LastPhaseVisitor(CaseStatistics stats) {
      stats_ = stats;
    }

    public Void visit(GatherStatsMessage message) {
      logger_.debug("Sending statistics to server.");
      networkQueue_.add(new StatsMessage(serverJobId_, null, server_, stats_));
      return null;
    }

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.debug("Received NewPhaseMessage in GatherStats state.");
    }
  }

  /**
   * Send my object's address to everyone and receive their information (used as a first phase
   * visitor in some test scenarios).
   *
   * @author Bolek Kulbabinski
   */
  protected final class AddressExchangeVisitor extends TestingModuleVisitor {
    private final List<CommAddress> clients_;
    private final int currClientIndex_;
    private final List<NebuloAddress> addresses_;
    private final NebuloAddress myAddress_;
    private final int initialSleep_;
    private final long timeoutMillis_;
    private final Timer visitorTimer_;

    public AddressExchangeVisitor(List<CommAddress> clients, List<NebuloAddress> addresses,
        int myClientId, NebuloAddress myAddress, int initialSleep, long timeoutMillis) {
      clients_ = new ArrayList<CommAddress>(clients);
      addresses_ = addresses;
      currClientIndex_ = myClientId;
      myAddress_ = myAddress;
      initialSleep_ = initialSleep;
      timeoutMillis_ = timeoutMillis;
      visitorTimer_ = timers_.get();
    }

    @Override
    public void visit(NewPhaseMessage message) {
      sleep(initialSleep_);
      logger_.debug("Sending NebuloAddress to " + clients_.size() + " peers.");
      for (int i = 0; i < clients_.size(); ++i) {
        if (i != currClientIndex_) {
          networkQueue_.add(new UserCommMessage(jobId_, clients_.get(i), myAddress_, phase_));
        }
      }
      visitorTimer_.schedule(jobId_, timeoutMillis_, String.valueOf(phase_));
      tryFinishPhase();
    }

    @Override
    protected void visitCorrectPhaseUserCommMessage(UserCommMessage message) {
      NebuloAddress receivedAddr = (NebuloAddress) message.getContent();
      logger_.debug("Received NebuloAddress: " + receivedAddr);
      addresses_.add(receivedAddr);
      tryFinishPhase();
    }

    @Override
    public void visit(TimeoutMessage message) {
      if (message.getMessageContent().equals(String.valueOf(phase_))) {
        logger_.debug("Finishing AddressExchangeVisitor in phase " + phase_ + " due to timeout." +
            "Received " + addresses_.size() + " addresses, expected " + (clients_.size() - 1));
        phaseFinished();
      } else {
        logger_.warn("Received TimeoutMessage from phase " + message.getMessageContent() +
            " while in phase " + phase_);
      }
    }

    private void tryFinishPhase() {
      if (addresses_.size() == clients_.size() - 1) {
        visitorTimer_.cancelTimer();
        phaseFinished();
      }
    }
  }
}
