package org.nebulostore.systest.async;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.systest.TestingPeer;

/**
 * Peer class for asynchronous messages test.
 *
 * @author Piotr Malicki
 *
 */
public class AsyncTestingPeer extends TestingPeer {

  private static Logger logger_ = Logger.getLogger(AsyncTestingPeer.class);

  private AsyncTestCommunicationOverlay communicationOverlay_;
  private CounterModule counterModule_;

  @Inject
  public void setCounterModule(CounterModule counterModule) {
    counterModule_ = counterModule;
  }

  @Inject
  public void setCommunicationOverlay(AsyncTestCommunicationOverlay communicationOverlay) {
    communicationOverlay_ = communicationOverlay;
  }

  @Override
  protected void initializeModules() {
    logger_.info("Starting testing peer with appKey = " + appKey_);
    runNetworkMonitor();
    runBroker();
    runAsyncMessaging();
    runCounterModule();
    runCommunicationOverlay();
  }

  private void runCounterModule() {
    counterModule_.runThroughDispatcher();
  }

  private void runCommunicationOverlay() {
    Thread commOverlayThread = new Thread(communicationOverlay_, "Communication overlay");
    commOverlayThread.start();
  }

  @Override
  public void quitNebuloStore() {
    super.quitNebuloStore();
    counterModule_.getInQueue().add(new EndModuleMessage());
    communicationOverlay_.getInQueue().add(new EndModuleMessage());
  }
}
