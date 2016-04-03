package org.nebulostore.crypto.session;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.UserMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.ContractList;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.session.message.DHFinishMessage;
import org.nebulostore.dht.core.KeyDHT;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;


/**
 * @author lukaszsiczek
 */
public class SessionChannelModule extends ReturningJobModule<SessionObjectMap> {

  private static final Logger LOGGER = Logger.getLogger(SessionChannelModule.class);

  private SessionChannelModuleVisitor sessionChannelModuleVisitor_ =
      new SessionChannelModuleVisitor();
  private AppKey appKey_;
  private List<ObjectId> objectIds_;
  private SessionObjectMap result_;
  private int startedSessions_;

  protected class SessionChannelModuleVisitor extends MessageVisitor {

    public void visit(JobInitMessage message) {
      networkQueue_.add(new GetDHTMessage(jobId_, new KeyDHT(appKey_.getKey())));
    }

    public void visit(ValueDHTMessage message) {
      UserMetadata metadata = (UserMetadata) message.getValue().getValue();
      LOGGER.debug("Metadata: " + metadata);
      ContractList contractList = metadata.getContractList();
      Set<CommAddress> instanceAddresses = new HashSet<CommAddress>();
      for (ObjectId objectId : objectIds_) {
        instanceAddresses.addAll(contractList.getGroup(objectId).getReplicators());
      }
      for (CommAddress commAddress : instanceAddresses) {
        startSession(commAddress);
      }
    }

    public void visit(DHFinishMessage message) {
      result_.put(message.getPeerAddress(), message.getSessionObject());
      if (result_.size() == startedSessions_) {
        endWithSuccess(result_);
      }
    }
  }

  private void startSession(CommAddress instance) {
    ++startedSessions_;
    SessionNegotiatorModule sessionNegotiatorModule =
        new SessionNegotiatorModule(instance, getJobId(), null, objectIds_.size());
    outQueue_.add(new JobInitMessage(sessionNegotiatorModule));
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(sessionChannelModuleVisitor_);
  }

  public void generateSessionChannelKey(AppKey appKey, List<ObjectId> objectIds) {
    appKey_ = appKey;
    objectIds_ = objectIds;
    result_ = new SessionObjectMap();
    runThroughDispatcher();
  }
}
