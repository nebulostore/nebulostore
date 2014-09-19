package org.nebulostore.async;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModuleFactory;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dht.messages.ErrorDHTMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;

/**
 * Class responsible for all functions of asynchronous messages sending. It involves:<br>
 * - initializing async messaging context<br>
 * - registering selection modules after discovery of a new peer<br>
 * - running synchronization module periodically<br>
 * - running cache refresh module periodically<br>
 *
 * @author Piotr Malicki
 */

public class AsyncMessagingModule extends JobModule {

  private static final long SYNCHRONIZATION_PERIOD_SEC = 5;
  private static final long CACHE_REFRESH_PERIOD_SEC = 10;
  private static final Logger LOGGER = Logger.getLogger(AsyncMessagingModule.class);

  private final SynchronizationService syncService_;
  private final ScheduledExecutorService synchronizationExecutor_;
  private final ScheduledExecutorService cacheRefreshExecutor_;
  private final NetworkMonitor networkMonitor_;
  private final BlockingQueue<Message> dispatcherQueue_;
  private final SynchroPeerSelectionModuleFactory selectionModuleFactory_;
  private final AsyncMessagesContext context_;
  private final CommAddress myAddress_;
  private final MessageVisitor<Void> visitor_ = new AsyngMessagingModuleVisitor();
  private final CacheRefreshingService cacheRefreshingService_;

  @Inject
  public AsyncMessagingModule(
      @Named("async.sync-executor")
        final ScheduledExecutorService synchronizationExecutor,
      @Named("async.cache-refresh-executor")
        final ScheduledExecutorService cacheRefreshExecutor,
      final NetworkMonitor networkMonitor,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      SynchroPeerSelectionModuleFactory selectionModuleFactory, AsyncMessagesContext context,
      CommAddress myAddress) {
    synchronizationExecutor_ = synchronizationExecutor;
    cacheRefreshExecutor_ = cacheRefreshExecutor;
    networkMonitor_ = networkMonitor;
    dispatcherQueue_ = dispatcherQueue;
    selectionModuleFactory_ = selectionModuleFactory;
    context_ = context;
    myAddress_ = myAddress;


    syncService_ = new SynchronizationService();
    cacheRefreshingService_ = new CacheRefreshingService();
  }

  private void startSynchronizationService() {
    synchronizationExecutor_.scheduleAtFixedRate(syncService_, 0, SYNCHRONIZATION_PERIOD_SEC,
        TimeUnit.SECONDS);
  }

  private void startCacheRefreshingService() {
    cacheRefreshExecutor_.scheduleAtFixedRate(cacheRefreshingService_, CACHE_REFRESH_PERIOD_SEC,
        CACHE_REFRESH_PERIOD_SEC, TimeUnit.SECONDS);
  }

  private class SynchronizationService implements Runnable {

    @Override
    public void run() {
      dispatcherQueue_.add(new JobInitMessage(new SynchronizeAsynchronousMessagesModule()));
    }
  }

  /**
   * Service that is responsible for refreshing cache of synchro-groups. It is run periodically.
   *
   * @author Piotr Malicki
   *
   */
  private class CacheRefreshingService implements Runnable {

    @Override
    public void run() {
      Set<CommAddress> recipients = context_.getRecipientsCopy();
      for (CommAddress recipient : recipients) {
        JobModule updateModule = new SynchroPeerSetUpdateModule(recipient, context_);
        updateModule.setOutQueue(dispatcherQueue_);
        updateModule.runThroughDispatcher();
      }
      context_.removeUnnecessarySynchroGroups();
    }

  }

  private enum ModuleState {
    INITIALIZING, RUNNING
  }

  protected class AsyngMessagingModuleVisitor extends MessageVisitor<Void> {

    private ModuleState state_ = ModuleState.INITIALIZING;

    public Void visit(JobInitMessage message) {
      // Run peer selection module when new peer is found.
      MessageGenerator addFoundSynchroPeer = new MessageGenerator() {
        @Override
        public Message generate() {
          return new JobInitMessage(selectionModuleFactory_.createModule());
        }
      };
      networkMonitor_.addContextChangeMessageGenerator(addFoundSynchroPeer);

      startSynchronizationService();
      startCacheRefreshingService();

      networkQueue_.add(new GetDHTMessage(jobId_, myAddress_.toKeyDHT()));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (state_.equals(ModuleState.INITIALIZING) &&
          message.getKey().equals(myAddress_.toKeyDHT())) {
        //TODO (pm) Maybe initialize cache here?
        if (message.getValue().getValue() instanceof InstanceMetadata) {
          InstanceMetadata metadata = (InstanceMetadata) message.getValue().getValue();
          LOGGER.debug("Received InstanceMetadata with synchro-set: " + metadata.getSynchroGroup() +
              " and recipients: " + metadata.getRecipients());
          context_.initialize(metadata.getSynchroGroup(), metadata.getRecipients());
          state_ = ModuleState.RUNNING;
        } else {
          LOGGER.warn("Received wrong type of message from DHT");
        }
      } else {
        LOGGER.warn("Received " + message.getClass() + " that was not expected");
      }
      return null;
    }

    public Void visit(ErrorDHTMessage message) {
      // no instance metadata in DHT, create empty context
      if (state_.equals(ModuleState.INITIALIZING)) {
        context_.initialize();
        state_ = ModuleState.RUNNING;
      } else {
        LOGGER.warn("Received " + message.getClass() + " that was not expected");
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
