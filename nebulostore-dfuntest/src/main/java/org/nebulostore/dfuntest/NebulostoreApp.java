package org.nebulostore.dfuntest;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import me.gregorias.dfuntest.App;
import me.gregorias.dfuntest.CommandException;
import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.RemoteProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java's proxy to Nebulostore external REST interface.
 *
 * @author Grzegorz Milka
 */
public class NebulostoreApp extends App<Environment> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NebulostoreApp.class);
  private final Environment env_;
  private final URI uri_;
  private final String javaCommand_;
  private RemoteProcess process_;

  public NebulostoreApp(int id, String name, Environment env, URI uri, String javaCommand) {
    super(id, name);
    env_ = env;
    uri_ = uri;
    javaCommand_ = javaCommand;
  }

  @Override
  public Environment getEnvironment() {
    return env_;
  }

  public Collection<String> getPeersList() throws IOException {
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(uri_).path("network_monitor/peers_list");

    String peersList;
    try {
      peersList = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
    } catch (ProcessingException e) {
      throw new IOException("Could not get peers list.", e);
    }
    Collection<String> peersListCollection = new ArrayList<>();
    for (JsonElement element : new JsonParser().parse(peersList).getAsJsonArray()) {
      peersListCollection.add(element.getAsString());
    }
    return peersListCollection;
  }

  @Override
  public void startUp() throws CommandException, IOException {
    LOGGER.debug("[{}] startUp()", getId());
    List<String> runCommand = new LinkedList<>();

    runCommand.add(javaCommand_);
    runCommand.add("-jar");
    runCommand.add(NebulostoreEnvironmentPreparator.JAR_FILE);
    process_ = env_.runCommandAsynchronously(runCommand);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void shutDown() throws IOException {
    LOGGER.debug("[{}] shutDown()", getId());
    if (process_ == null) {
      throw new IllegalStateException("Nebulostore is not running.");
    }
    process_.destroy();

    /*try {
      terminate();
      try {
        process_.waitFor();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } catch (IOException e) {
      process_.destroy();
    }*/
  }

  private void terminate() throws IOException {
    LOGGER.debug("terminate()");
    Client client = ClientBuilder.newClient();
    WebTarget target = client.target(uri_).path("terminate");

    String keyStr;
    try {
      keyStr = target.request(MediaType.APPLICATION_JSON).get(String.class);
    } catch (ProcessingException e) {
      LOGGER.error("terminate()", e);
      throw new IOException("Could not get key.", e);
    }
    LOGGER.debug("terminate() -> void", keyStr);
    return;
  }
}
