package org.nebulostore.async.synchrogroup;

import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.async.synchrogroup.messages.SynchroPeerSetUpdateJobEndedMessage;
import org.nebulostore.async.util.RecipientsData;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.core.ValueDHT;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.OkDHTMessage;
import org.nebulostore.dht.messages.PutDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.timer.TimeoutMessage;
import org.nebulostore.timer.Timer;

/**
 * Module that refreshes synchro group cache in in the asynchronous messages context. After
 * refreshing the cache, it updates current instance's recipients set in the context and DHT.
 *
 * @author Piotr Malicki
 *
 */
public class CacheRefreshingModule extends JobModule {

  private static final long CACHE_REFRESH_TIMEOUT_MILIS = 4000;

  private static Logger logger_ = Logger.getLogger(CacheRefreshingModule.class);

  private final MessageVisitor<Void> visitor_ = new CacheRefreshingModuleVisitor();

  private final Set<String> modulesIds_ = new HashSet<String>();

  private AsyncMessagesContext context_;
  private Timer timer_;
  private CommAddress myAddress_;
  private AppKey appKey_;

  @Inject
  public void setDependencies(AsyncMessagesContext context, Timer timer, CommAddress myAddress,
      AppKey appKey) {
    context_ = context;
    timer_ = timer;
    myAddress_ = myAddress;
    appKey_ = appKey;
  }

  private enum ModuleState {
    WAITING_FOR_RESPONSES, UPDATING_DHT
  }

  protected class CacheRefreshingModuleVisitor extends MessageVisitor<Void> {

    private ModuleState state_ = ModuleState.WAITING_FOR_RESPONSES;

    public Void visit(JobInitMessage message) {
      if (context_.isInitialized()) {
        Set<CommAddress> recipients = context_.getRecipientsData().getRecipients();
        for (CommAddress recipient : recipients) {
          JobModule updateModule = new SynchroPeerSetUpdateModule(recipient, context_, inQueue_);
          updateModule.setOutQueue(outQueue_);
          updateModule.runThroughDispatcher();
          modulesIds_.add(updateModule.getJobId());
        }

        if (modulesIds_.isEmpty()) {
          finishModule();
        } else {
          logger_.debug("Synchro-peer set update modules' ids: " + modulesIds_);
          timer_.schedule(jobId_, CACHE_REFRESH_TIMEOUT_MILIS);
        }
      } else {
        logger_.warn("Asynchronous messages context has not yet been initialized." +
            "Ending the module.");
        finishModule();
      }

      return null;
    }

    public Void visit(SynchroPeerSetUpdateJobEndedMessage message) {
      logger_.debug("Module with id: " + message.getId() + " ended.");
      if (state_.equals(ModuleState.WAITING_FOR_RESPONSES)) {
        modulesIds_.remove(message.getId());
        if (modulesIds_.isEmpty()) {
          logger_.debug("All modules ended, updating recipients in DHT");
          timer_.cancelTimer();
          updateRecipientsInDHT();
        }
      }
      return null;
    }

    public Void visit(TimeoutMessage message) {
      if (state_.equals(ModuleState.WAITING_FOR_RESPONSES)) {
        logger_.warn("Timeout in " + CacheRefreshingModule.class.getSimpleName());
        state_ = ModuleState.UPDATING_DHT;
        updateRecipientsInDHT();
      }
      return null;
    }

    public Void visit(OkDHTMessage message) {
      if (state_.equals(ModuleState.UPDATING_DHT)) {
        logger_.info("Successfully refreshed synchro group sets");
        endJobModule();
      } else {
        logger_.warn("Received unexpected " + message.getClass().getName());
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      if (state_.equals(ModuleState.UPDATING_DHT)) {
        logger_.warn("Updating recipients set in DHT failed");
        endJobModule();
      } else {
        logger_.warn("Received unexpected " + message.getClass().getName());
      }
      return null;
    }

    private void updateRecipientsInDHT() {
      state_ = ModuleState.UPDATING_DHT;
      context_.removeUnnecessarySynchroGroups();

      InstanceMetadata metadata = new InstanceMetadata(appKey_);
      RecipientsData recipientsData = context_.getRecipientsData();
      metadata.setRecipients(recipientsData.getRecipients());
      metadata.setRecipientsSetVersion(recipientsData.getRecipientsSetVersion());
      networkQueue_.add(new PutDHTMessage(jobId_, myAddress_.toKeyDHT(), new ValueDHT(metadata)));
    }

    private void finishModule() {
      timer_.cancelTimer();
      endJobModule();
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
