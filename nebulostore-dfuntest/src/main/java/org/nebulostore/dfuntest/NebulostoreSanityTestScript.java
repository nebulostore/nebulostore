package org.nebulostore.dfuntest;

import java.io.IOException;
import java.util.Collection;

import me.gregorias.dfuntest.CommandException;
import me.gregorias.dfuntest.TestResult;
import me.gregorias.dfuntest.TestResult.Type;
import me.gregorias.dfuntest.TestScript;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test checks simply whether nebulostore boots up and shut down.
 *
 * @author Grzegorz Milka
 */
public class NebulostoreSanityTestScript implements TestScript<NebulostoreApp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreSanityTestScript.class);

  @Override
  public TestResult run(final Collection<NebulostoreApp> apps) {
    for (NebulostoreApp app : apps) {
      try {
        app.startUp();
      } catch (CommandException | IOException e) {
        return new TestResult(Type.FAILURE, "Could not start up application." + e);
      }

      LOGGER.info("run(): Sleeping.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
      }
      LOGGER.info("run(): Waking up.");

      try {
        app.shutDown();
      } catch (IOException e) {
        LOGGER.warn("run(): shutDown has thrown an exception.", e);
      }
    }
    return new TestResult(Type.SUCCESS, "App has started successfully.");
  }

  @Override
  public String toString() {
    return "NebulostoreSanityTestScript";
  }
}
