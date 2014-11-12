package org.nebulostore.dfuntest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import me.gregorias.dfuntest.CommandException;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;
import me.gregorias.dfuntest.TestScript;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test runs nebulostore instances and checks whether peers list (as returned by REST)
 * contains at least some other hosts than itself.
 *
 * @author Grzegorz Milka
 */
public class NebulostorePeersListTestScript implements TestScript<NebulostoreApp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      NebulostorePeersListTestScript.class);

  @Override
  public TestResult run(final Collection<NebulostoreApp> apps) {
    Collection<NebulostoreApp> startedApps = new ArrayList<>();
    TestResult result = new TestResult(Type.SUCCESS, "PeersList test was successful");
    for (NebulostoreApp app : apps) {
      try {
        app.startUp();
        startedApps.add(app);
      } catch (CommandException | IOException e) {
        for (NebulostoreApp startedApp : startedApps) {
          try {
            startedApp.shutDown();
          } catch (IOException e1) {
            LOGGER.warn("run()", e);
          }
        }

        return new TestResult(Type.FAILURE, "Could not start up application." + e);
      }
    }

    LOGGER.info("run(): Sleeping.");
    try {
      Thread.sleep(50000);
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
    }
    LOGGER.info("run(): Waking up.");

    final int expectedSizeOfPeersList = apps.size() <= 1 ? 1 : 2;
    for (NebulostoreApp app : apps) {
      try {
        Collection<String> peersList = app.getPeersList();
        LOGGER.info("run: Got peers list from app[{}]: {}", app.getId(), peersList);
        if (peersList.size() < expectedSizeOfPeersList) {
          result = new TestResult(Type.FAILURE, "Expected bigger peers list from app[" +
            app.getId() + "]");
          break;
        }
      } catch (IOException e) {
        LOGGER.error("run(): Error when getting peers list.", e);
      }
    }

    for (NebulostoreApp app : apps) {
      try {
        app.shutDown();
      } catch (IOException e) {
        LOGGER.warn("run(): shutDown has thrown an exception.", e);
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return "NebulostorePeersListTestScript";
  }
}
