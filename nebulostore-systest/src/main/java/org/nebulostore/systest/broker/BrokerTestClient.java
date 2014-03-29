package org.nebulostore.systest.broker;

import com.google.inject.Inject;
import com.google.inject.Provider;

import org.apache.log4j.Logger;
import org.nebulostore.broker.Broker;
import org.nebulostore.broker.BrokerContext;
import org.nebulostore.broker.Contract;
import org.nebulostore.broker.ContractsSet;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.GatherStatsMessage;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.StatsMessage;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.systest.broker.messages.BrokerContextMessage;
import org.nebulostore.systest.broker.messages.GetBrokerContextMessage;
import org.nebulostore.systest.messages.ChangeTestMessageHandlerMessage;
import org.nebulostore.systest.networkmonitor.FaultyConnectionTestMessageHandler;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Sets up ConnectionTestMesssageHandler and waits.
 * @author szymonmatejczyk
 */
public class BrokerTestClient extends ConductorClient {
  private static final long serialVersionUID = -7209499692156491320L;
  private static Logger logger_ = Logger.getLogger(BrokerTestClient.class);

  private static final int FIRST_PHASE_TIME_SEC = 15;
  private static final int SECOND_PHASE_TIME_SEC = 45;


  private static final int INITIAL_SLEEP = 4000;

  private Timer timer_;
  private Broker broker_;

  private double availability_;

  @Inject
  public void setDependencies(Timer timer, Broker broker) {
    timer_ = timer;
    broker_ = broker;
  }

  public BrokerTestClient(String serverJobId, int numPhases, CommAddress serverCommAddress,
      double availability) {
    super(serverJobId, numPhases, serverCommAddress);
    availability_ = availability;
  }

  @Override
  protected void initVisitors() {
    // Setting NetworkMonitor to imitate failures.
    Provider<ConnectionTestMessageHandler> providerOfFaultyHandler =
        new Provider<ConnectionTestMessageHandler>() {
        @Override
        public ConnectionTestMessageHandler get() {
          return new FaultyConnectionTestMessageHandler(availability_);
        }
      };
    outQueue_.add(new ChangeTestMessageHandlerMessage(providerOfFaultyHandler));
    sleep(INITIAL_SLEEP);
    visitors_ = new TestingModuleVisitor[numPhases_ + 2];
    visitors_[0] = new EmptyInitializationVisitor();
    visitors_[1] = new SimpleContractsConclusionCheckVisitor(1000L * FIRST_PHASE_TIME_SEC, timer_);
    visitors_[2] = new DelayingVisitor(1000L * SECOND_PHASE_TIME_SEC, timer_);
    visitors_[3] = new BrokerLastPhaseVisitor();
  }

  /**
   * Visitor.
   */
  protected class SimpleContractsConclusionCheckVisitor extends DelayingVisitor {
    public SimpleContractsConclusionCheckVisitor(long delayTime, Timer timer) {
      super(delayTime, timer);
    }

    @Override
    public Void visit(TimeoutMessage message) {
      logger_.debug("Phase delaying finished.");
      broker_.getInQueue().add(new GetBrokerContextMessage(jobId_));
      return null;
    }

    public Void visit(BrokerContextMessage message) {
      assertTrue(message.getBrokerContext().getReplicas().length > 0,
          "No contracts concluded in the first phase");
      phaseFinished();
      return null;
    }
  }

  /**
   * Sends statistics gathered from DHT to server.
   */
  protected class BrokerLastPhaseVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      logger_.debug("Received NewPhaseMessage in GatherStats state.");
      return null;
    }

    public Void visit(GatherStatsMessage message) {
      logger_.debug("Gathering statistics...");
      broker_.getInQueue().add(new GetBrokerContextMessage(jobId_));
      return null;
    }

    public Void visit(BrokerContextMessage message) {
      logger_.debug("Got broker context.");
      BrokerTestStatistics stats = new BrokerTestStatistics();
      BrokerContext brokerContext = message.getBrokerContext();

      ContractsSet contracts = brokerContext.acquireReadAccessToContracts();
      for (Contract contract : contracts) {
        stats.addContract(contract);
      }
      brokerContext.disposeReadAccessToContracts();

      networkQueue_.add(new StatsMessage(serverJobId_, null, server_, stats));
      return null;
    }

  }
}
