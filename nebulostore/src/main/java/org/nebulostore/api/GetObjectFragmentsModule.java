package org.nebulostore.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;

import org.apache.log4j.Logger;
import org.nebulostore.api.GetObjectFragmentsModule.FragmentsData;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.timer.TimeoutMessage;

/**
 * Module that retrieves object fragments from replicators given in the constructor. Returns only
 * fragments with the newest object version found paired with this version.
 *
 * @author Piotr Malicki
 */
public class GetObjectFragmentsModule extends GetModule<FragmentsData> {

  private static Logger logger_ = Logger.getLogger(GetObjectFragmentsModule.class);

  private final Set<CommAddress> replicators_ = new HashSet<>();

  public GetObjectFragmentsModule(NebuloAddress address, Collection<CommAddress> replicators,
      BlockingQueue<Message> dispatcherQeue) {
    address_ = address;
    replicators_.addAll(replicators);
    outQueue_ = dispatcherQeue;
  }

  private final MessageVisitor visitor_ = new GetObjectFragmentsVisitor();

  protected class GetObjectFragmentsVisitor extends GetModuleVisitor {

    private final Map<CommAddress, EncryptedObject> result_ = new HashMap<>();

    @Override
    public void visit(JobInitMessage message) {
      replicationGroupList_ = Lists.newArrayList(replicators_);
      queryNextReplicas();
    }

    @Override
    public void visit(SendObjectMessage message) {
      if (replicators_.remove(message.getSourceAddress())) {
        try {
          if (checkVersion(message)) {
            result_.clear();
          }
        } catch (NebuloException e) {
          logger_.warn(e);
        }

        EncryptedObject fragment;
        try {
          fragment = decryptWithSessionKey(message.getEncryptedEntity(), message.getSessionId());
          result_.put(message.getSourceAddress(), fragment);
          if (replicators_.isEmpty()) {
            endWithResult();
          }
        } catch (CryptoException e) {
          logger_.warn("Error when trying to decrypt object.", e);
        }
      } else {
        logger_.warn("Received an unexpected object fragment.");
      }
    }

    @Override
    public void visit(TimeoutMessage message) {
      endWithResult();
    }

    @Override
    protected void failReplicator(CommAddress replicator) {
      if (replicators_.remove(replicator)) {
        logger_.warn("Could not fetch object fragment from peer " + replicator);
        tryEndModule();
      }
    }

    private void tryEndModule() {
      if (replicators_.isEmpty()) {
        endWithResult();
      }
    }

    private void endWithResult() {
      endWithSuccess(new FragmentsData(result_, currentVersions_));
    }
  }

  public class FragmentsData {
    public Map<CommAddress, EncryptedObject> fragmentsMap_;
    public List<String> versions_;

    public FragmentsData(Map<CommAddress, EncryptedObject> fragmentsMap, List<String> versions) {
      fragmentsMap_ = fragmentsMap;
      versions_ = versions;

    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
