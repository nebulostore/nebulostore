package org.nebulostore.dfuntest;

import java.net.URI;
import java.net.URISyntaxException;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.ApplicationFactory;
import me.gregorias.dfuntest.Environment;

public class NebulostoreAppFactory implements ApplicationFactory<Environment, NebulostoreApp> {
  public static final String JAVA_COMMAND_ARGUMENT_NAME = "NebulostoreAppFactory.javaCommand";

  private final String javaCommand_;

  @Inject
  public NebulostoreAppFactory(@Named(JAVA_COMMAND_ARGUMENT_NAME) String javaCommand) {
    javaCommand_ = javaCommand;
  }

  @Override
  public NebulostoreApp newApp(Environment env) {
    String host = env.getHostname();
    int port = (Integer) env.getProperty(
        NebulostoreEnvironmentPreparator.XML_REST_PORT_FIELD);
    URI uri;
    try {
      uri = new URI("http", null, host, port, null, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Could not create valid URI.", e);
    }
    return new NebulostoreApp(env.getId(), String.format("Nebulostore[%d]",
        env.getId()),
        env,
        uri,
        javaCommand_);
  }
}
