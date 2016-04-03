package org.nebulostore.systest;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.conductor.ConductorServer;
import org.nebulostore.peers.Peer;

/**
 * Class that runs test server. If "systest.is-server" option is set to false it behaves like
 * standard Peer.
 *
 * @author Bolek Kulbabinski
 */
public class TestingPeer extends Peer {
  private static Logger logger_ = Logger.getLogger(TestingPeer.class);
  protected static final String CLASS_LIST_CONFIG = "systest.testing-peer-class-list";
  protected static final String IS_SERVER_CONFIG = "systest.is-server";
  protected static final String N_TEST_PARTICIPANTS_CONFIG = "systest.num-test-participants";

  protected String[] testClasses_;
  protected int nTestParticipants_;
  protected boolean isTestServer_;

  @Inject
  public void setTestParameters(@Named(N_TEST_PARTICIPANTS_CONFIG) int nTestParticipants,
      @Named(CLASS_LIST_CONFIG) String classList, @Named(IS_SERVER_CONFIG) boolean isServer) {
    nTestParticipants_ = nTestParticipants;
    if (nTestParticipants_ <= 1) {
      throw new RuntimeException("Illegal number of test participants! (" +
          nTestParticipants + ")");
    }
    testClasses_ = classList.split(";");
    if (testClasses_.length == 0) {
      throw new RuntimeException("Empty test classes list!");
    }
    isTestServer_ = isServer;
  }

  @Override
  protected void initializeModules() {
    logger_.info("Starting testing peer with appKey = " + appKey_);
    runNetworkMonitor();
    runBroker();
    runAsyncMessaging();
  }

  @Override
  protected void runActively() {
    // TODO: Move register to separate module or at least make it non-blocking.
    register();
    if (isTestServer_) {
      runTestingServer();
    }
  }

  @Override
  protected void cleanModules() {
    System.exit(0);
  }

  protected void runTestingServer() {
    logger_.info("Running " + testClasses_.length + " tests.");
    for (String className : testClasses_) {
      ConductorServer testServer = null;
      try {
        testServer = (ConductorServer) Class.forName(className).newInstance();
        injector_.injectMembers(testServer);
        logger_.info("Starting " + className + " test.");
        if (runTest(testServer, className)) {
          logger_.info("Test " + className + " succeeded!");
        } else {
          fatal("Test " + className + " failed!");
        }
      } catch (InstantiationException e) {
        fatal("Could not instantiate class " + className + ".");
      } catch (IllegalAccessException e) {
        fatal("Constructor for class " + className + " is not accessible.");
      } catch (ClassNotFoundException e) {
        fatal("Class " + className + " not found.");
      }
    }
    logger_.info("All tests finished successfully.");
    quitNebuloStore();
  }

  protected void fatal(String message) {
    logger_.fatal(message);
    System.exit(1);
  }

  private boolean runTest(ConductorServer testServer, String testName) {
    try {
      testServer.runThroughDispatcher();
      testServer.getResult();
      return true;
    } catch (NebuloException exception) {
      logger_.error("NebuloException at test " + testName + " : " + exception.getMessage());
      return false;
    }
  }
}
