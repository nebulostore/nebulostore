package org.nebulostore.async;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.peerselection.SynchroPeerSelectionModuleFactory;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.networkmonitor.NetworkMonitor;
import org.nebulostore.timer.MessageGenerator;

/**
 * Class responsible for all functions of asynchronous messages sending.
 * It involves:<br>
 * - initializing async messaging context<br>
 * - registering selection modules after discovery of a new peer<br>
 * - running synchronization module periodically
 *
 * @author Piotr Malicki
 */

public class AsyncMessagingModule {

  private static final long SYNCHRONIZATION_PERIOD = 5;

  private final SynchronizationService syncService_;
  private final ScheduledExecutorService synchronizationExecutor_;
  private final NetworkMonitor networkMonitor_;
  private final BlockingQueue<Message> dispatcherQueue_;
  private final SynchroPeerSelectionModuleFactory selectionModuleFactory_;

  @Inject
  public AsyncMessagingModule(
      @Named("async.scheduled-executor-service")
      final ScheduledExecutorService synchronizationExecutor,
      final NetworkMonitor networkMonitor,
      @Named("DispatcherQueue") BlockingQueue<Message> dispatcherQueue,
      SynchroPeerSelectionModuleFactory selectionModuleFactory) {
    synchronizationExecutor_ = synchronizationExecutor;
    networkMonitor_ = networkMonitor;
    dispatcherQueue_ = dispatcherQueue;
    selectionModuleFactory_ = selectionModuleFactory;

    syncService_ = new SynchronizationService();
  }

  public void startUp() {
    JobModule module = new AsyncMessagesContextInitializationModule();
    dispatcherQueue_.add(new JobInitMessage(module));

    // Run peer selection module when new peer is found.
    MessageGenerator addFoundSynchroPeer = new MessageGenerator() {
      @Override
      public Message generate() {
          return new JobInitMessage(selectionModuleFactory_.createModule());
        }
    };
    networkMonitor_.addContextChangeMessageGenerator(addFoundSynchroPeer);

    startSynchronizationService();

  }

  private void startSynchronizationService() {
    synchronizationExecutor_.scheduleAtFixedRate(syncService_, 0, SYNCHRONIZATION_PERIOD,
        TimeUnit.SECONDS);
  }

  private class SynchronizationService implements Runnable {

    @Override
    public void run() {
      dispatcherQueue_.add(new JobInitMessage(new SynchronizeAsynchronousMessagesModule()));
    }
  }

}
