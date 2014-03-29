package org.nebulostore.systest.networkmonitor;

import java.util.List;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import com.google.inject.Provider;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.GatherStatsMessage;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.StatsMessage;
import org.nebulostore.networkmonitor.ConnectionAttribute;
import org.nebulostore.networkmonitor.ConnectionTestMessageHandler;
import org.nebulostore.networkmonitor.GetPeersConnectionAverageStatisticModule;
import org.nebulostore.networkmonitor.PeerConnectionSurvey;
import org.nebulostore.systest.messages.ChangeTestMessageHandlerMessage;
import org.nebulostore.timer.Timer;

/**
 * Sets up ConnectionTestMesssageHandler and waits.
 * @author szymonmatejczyk
 */
public class NetworkMonitorTestClient extends ConductorClient {
  private static final long serialVersionUID = -7209499692156491320L;
  private static Logger logger_ = Logger.getLogger(NetworkMonitorTestClient.class);

  private static final int MONITORING_TIME_SECS = 75;

  private static final int GET_STATISTICS_TIMEOUT_SECS = 4;

  private static final int INITIAL_SLEEP = 1000;

  private long testStartTime_;

  /**
   * Availability simulated by this peer.
   */
  private final double availability_;

  /**
   * Other peers statistics that this peer has obtained.
   */
  private final NetworkMonitorStatistics stats_;

  /**
   * Clients known by this peer.
   */
  private final List<CommAddress> clients_;

  private Timer timer_;
  private CommAddress myAddress_;

  @Inject
  public void setDependencies(Timer timer, CommAddress commAddress) {
    timer_ = timer;
    myAddress_ = commAddress;
  }

  public NetworkMonitorTestClient(String serverJobId, int numPhases, CommAddress serverCommAddress,
      List<CommAddress> clients, double availability) {
    super(serverJobId, numPhases, serverCommAddress);
    availability_ = availability;
    clients_ = clients;
    stats_ = new NetworkMonitorStatistics();
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
    visitors_[1] = new DelayingVisitor(1000L * MONITORING_TIME_SECS, timer_);
    visitors_[2] = new NetworkMonitorLastPhaseVisitor();
  }

  /**
   * Sends statistics gathered from DHT to server.
   */
  protected class NetworkMonitorLastPhaseVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      logger_.debug("Received NewPhaseMessage in GatherStats state.");
      return null;
    }

    public Void visit(GatherStatsMessage message) {
      logger_.debug("Gathering statistics...");
      if (gatherNetworkMonitorStatistics()) {
        networkQueue_.add(new StatsMessage(serverJobId_, null, server_, stats_));
      }
      return null;
    }

  }

   /**
   * Tries to gather statistics for known clients. Return true iff it succeeds.
   */
  private boolean gatherNetworkMonitorStatistics() {
    double availability;
    for (CommAddress client : clients_) {
      if (!client.equals(myAddress_)) {
        Predicate<PeerConnectionSurvey> availabilityAfterStartTimeFilter =
          new Predicate<PeerConnectionSurvey>() {
            @Override
            public boolean apply(PeerConnectionSurvey t) {
              return t.getAttribute() == ConnectionAttribute.AVAILABILITY &&
                  t.getTime() > testStartTime_;
            }
          };

        GetPeersConnectionAverageStatisticModule getAvailabilityModule =
            new GetPeersConnectionAverageStatisticModule(client, availabilityAfterStartTimeFilter);
        getAvailabilityModule.setDispatcherQueue(outQueue_);
        getAvailabilityModule.runThroughDispatcher();
        try {
          availability = getAvailabilityModule.getResult(GET_STATISTICS_TIMEOUT_SECS);
          stats_.addEstimatedAvailability(client, availability);
          logger_.debug("Retrived + " + client.toString() + " statistics.");
        } catch (NebuloException e) {
          endWithError("Unable to retrieve peer: " + client + " statistics.");
          return false;
        }
      }
    }
    return true;
  }
}
