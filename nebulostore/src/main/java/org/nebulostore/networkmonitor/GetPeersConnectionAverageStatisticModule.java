package org.nebulostore.networkmonitor;

import java.util.Queue;

import com.google.common.base.Predicate;
import com.google.inject.Inject;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Retrives from DHT the average of a given connectionAttribute for peer.
 *
 * @author szymonmatejczyk
 */
public class GetPeersConnectionAverageStatisticModule extends ReturningJobModule<Double> {
  private static Logger logger_ = Logger.getLogger(GetPeersConnectionAverageStatisticModule.class);
  /**
   * Peer we retrieve statistics of.
   */
  private final CommAddress peer_;


  private final Predicate<PeerConnectionSurvey> predicate_;

  public GetPeersConnectionAverageStatisticModule(CommAddress peer,
      Predicate<PeerConnectionSurvey> filter) {
    peer_ = peer;
    predicate_ = filter;
  }

  private static final String CONFIGURATION_PREFIX = "networkmonitor.";
  private XMLConfiguration config_;

  @Inject
  private void setConfig(XMLConfiguration config) {
    config_ = config;
  }

  private final MessageVisitor<Void> visitor_ = new GetStatisticsModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  /**
   * Visitor.
   */
  protected class GetStatisticsModuleVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      RetrievePeersStatistics allStatsModule = new RetrievePeersStatistics(peer_, outQueue_);
      try {
        Queue<PeerConnectionSurvey> allStats = allStatsModule.getResult(config_
            .getInt(CONFIGURATION_PREFIX + "get-stats-timeout-secs"));
        double sum = 0;
        int count = 0;
        for (PeerConnectionSurvey survey : allStats) {
          if (predicate_.apply(survey)) {
            sum += survey.getValue();
            count++;
          }
        }
        logger_.debug("Averago of: " + count);
        endWithSuccess(sum / count);
      } catch (NebuloException exception) {
        endWithError(exception);
      }
      return null;
    }
  }

}
