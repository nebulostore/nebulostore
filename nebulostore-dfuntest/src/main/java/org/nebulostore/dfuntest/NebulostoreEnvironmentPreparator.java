package org.nebulostore.dfuntest;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import me.gregorias.dfuntest.Environment;
import me.gregorias.dfuntest.EnvironmentPreparator;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Preparator of testing environments for Nebulostore tests.
 *
 * This preparator prepares XML configuration file expected by Nebulostore and sets up environment
 * properties for {@link NebulostoreAppFactory}.
 *
 * It assumes that:
 * <ul>
 * <li> All required dependency libraries are in lib/ directory. </li>
 * <li> Cryptographic keys are in keys/ directory. </li>
 * <li> Nebulostore is in Nebulostore.jar file. </li>
 * </ul>
 *
 * @author Grzegorz Milka
 */
public class NebulostoreEnvironmentPreparator implements EnvironmentPreparator<Environment> {
  public static final String LOG_DIRECTORY = "logs";
  public static final String KEYS_DIRECTORY = "keys/";
  public static final String RESOURCES_DIRECTORY = "resources";
  public static final String LIB_DIRECTORY = "lib";
  public static final String JAR_FILE = "Nebulostore.jar";

  public static final String COMM_INITIAL_PORT_ARGUMENT_NAME =
      "NebulostoreEnvironmentPreparator.commInitialPort";
  public static final String REST_INITIAL_PORT_ARGUMENT_NAME =
      "NebulostoreEnvironmentPreparator.restInitialPort";

  public static final String XML_REST_PORT_FIELD = "rest-api.server-config.port";
  private static final Logger LOGGER = LoggerFactory.getLogger(
      NebulostoreEnvironmentPreparator.class);
  private static final Path XML_CONFIG_BASE_PATH =
      FileSystems.getDefault().getPath("resources/conf/Peer.xml");
  private static final Path XML_CONFIG_TARGET_PATH =
      FileSystems.getDefault().getPath("resources/conf/Peer.xml");
  private static final Path RESOURCES_PATH = FileSystems.getDefault().getPath(RESOURCES_DIRECTORY);
  private static final Path LOCAL_JAR_PATH = FileSystems.getDefault().getPath(JAR_FILE);
  private static final Path LOCAL_KEYS_PATH = FileSystems.getDefault().getPath(KEYS_DIRECTORY);
  private static final Path LOCAL_LIBS_PATH = FileSystems.getDefault().getPath(LIB_DIRECTORY);

  private static final int KEY_SIZE = 4096;

  private final int commInitialPort_;
  private final int restInitialPort_;

  @Inject
  public NebulostoreEnvironmentPreparator(
      @Named(COMM_INITIAL_PORT_ARGUMENT_NAME) int commInitialPort,
      @Named(REST_INITIAL_PORT_ARGUMENT_NAME) int restInitialPort) {
    commInitialPort_ = commInitialPort;
    restInitialPort_ = restInitialPort;
  }

  @Override
  public void cleanAll(Collection<Environment> envs) {
    cleanOutput(envs);
    for (Environment env : envs) {
      try {
        env.removeFile(LIB_DIRECTORY);
        env.removeFile(KEYS_DIRECTORY);
        env.removeFile(JAR_FILE);
        env.removeFile(RESOURCES_DIRECTORY);
      } catch (InterruptedException | IOException e) {
        LOGGER.warn("cleanAll(): Could not remove some files on {}.", env.getId(), e);
      }
    }
  }

  @Override
  public void cleanOutput(Collection<Environment> envs) {
    for (Environment env : envs) {
      try {
        env.removeFile(NebulostoreEnvironmentPreparator.LOG_DIRECTORY);
      } catch (IOException | InterruptedException e) {
        LOGGER.warn("cleanOutput(): Could not remove logs.", e);
      }
    }
  }

  @Override
  public void collectOutput(Collection<Environment> envs, Path destPath) {
    for (Environment env : envs) {
      try {
        env.copyFilesToLocalDisk(LOG_DIRECTORY, destPath.resolve("" + env.getId()));
      } catch (IOException e) {
        LOGGER.warn("collectOutputAndLogFiles(): Could not collect logs.", e);
      }
    }
  }

  @Override
  public void prepare(Collection<Environment> envs) throws IOException {
    LOGGER.info("prepareEnvironments()");
    Collection<Environment> preparedEnvs = new LinkedList<>();
    Environment zeroEnvironment = findZeroEnvironment(envs);
    for (Environment env : envs) {
      try {
        XMLConfiguration xmlConfig = prepareXMLAndEnvConfiguration(env, zeroEnvironment);
        xmlConfig.save(XML_CONFIG_TARGET_PATH.toFile());
        prepareCryptographicKeys();
        String targetPath = ".";
        env.copyFilesFromLocalDisk(RESOURCES_PATH, targetPath);
        env.copyFilesFromLocalDisk(LOCAL_KEYS_PATH.toAbsolutePath(), targetPath);
        env.copyFilesFromLocalDisk(LOCAL_JAR_PATH.toAbsolutePath(), targetPath);
        env.copyFilesFromLocalDisk(LOCAL_LIBS_PATH.toAbsolutePath(), targetPath);
        preparedEnvs.add(env);
      } catch (ConfigurationException | IOException | InterruptedException e) {
        cleanAll(preparedEnvs);
        LOGGER.error("prepareEnvironments() -> Could not prepare environment.", e);
        throw new IOException(e);
      }
    }
  }

  @Override
  public void restore(Collection<Environment> envs) throws IOException {
    // TODO Auto-generated method stub
  }

  private String createUID(int id) {
    return String.format("00000000-0000-0000-%04d-000000000000", id);
  }

  private Environment findZeroEnvironment(Collection<Environment> envs) {
    for (Environment env : envs) {
      if (env.getId() == 0) {
        return env;
      }
    }
    throw new NoSuchElementException("No zero environment present");
  }

  private void prepareCryptographicKeys() throws IOException, InterruptedException {
    String privatePEM = KEYS_DIRECTORY + "private.pem";
    Path privatePEMPath = FileSystems.getDefault().getPath(privatePEM);
    String privateKey = KEYS_DIRECTORY + "private.key";
    String publicKey = KEYS_DIRECTORY + "public.key";

    if (!Files.isDirectory(LOCAL_KEYS_PATH)) {
      Files.createDirectory(LOCAL_KEYS_PATH);
    }
    ProcessBuilder pb = new ProcessBuilder();
    pb.command("openssl", "genrsa", "-out", privatePEM, Integer.toString(KEY_SIZE));
    Process proc = pb.start();
    proc.waitFor();

    pb.command("openssl", "pkcs8", "-topk8", "-inform", "PEM", "-outform" , "DER", "-in",
        privatePEM, "-out", privateKey, "-nocrypt");
    proc = pb.start();
    proc.waitFor();

    pb.command("openssl", "rsa", "-in", privatePEM, "-pubout", "-outform", "DER", "-out",
        publicKey);
    proc = pb.start();
    proc.waitFor();

    Files.delete(privatePEMPath);
  }

  private XMLConfiguration prepareXMLAndEnvConfiguration(Environment env,
      Environment zeroEnv) throws ConfigurationException {
    XMLConfiguration xmlConfig = new XMLConfiguration(XML_CONFIG_BASE_PATH.toFile());
    xmlConfig.setProperty("app-key", String.format("%d%d", env.getId(), env.getId()));
    xmlConfig.setProperty("class-name", "org.nebulostore.peers.Peer");
    xmlConfig.setProperty("configuration-class-name", "org.nebulostore.peers.PeerConfiguration");

    xmlConfig.setProperty("communication.local-net-address", env.getHostname());
    xmlConfig.setProperty("communication.bootstrap-net-address", zeroEnv.getHostname());
    xmlConfig.setProperty("communication.comm-address", createUID(env.getId()));
    xmlConfig.setProperty("communication.bootstrap-comm-address", createUID(zeroEnv.getId()));
    xmlConfig.setProperty("communication.ports.comm-cli-port", commInitialPort_ + env.getId());
    xmlConfig.setProperty("communication.ports.bootstrap-port", commInitialPort_ + zeroEnv.getId());
    xmlConfig.setProperty("communication.ports.tomp2p-port", 12000 + env.getId());
    xmlConfig.setProperty("communication.ports.bootstrap-server-tomp2p-port",
        12000 + zeroEnv.getId());

    if (env.equals(zeroEnv)) {
      xmlConfig.setProperty("communication.bootstrap.mode", "server");
      xmlConfig.setProperty("communication.dht.bdb-peer.type", "storage-holder");
      xmlConfig.setProperty("communication.remotemap.mode", "server");
    } else {
      xmlConfig.setProperty("communication.bootstrap.mode", "client");
      xmlConfig.setProperty("communication.dht.bdb-peer.type", "proxy");
      xmlConfig.setProperty("communication.remotemap.mode", "client");
    }
    xmlConfig.setProperty("communication.bootstrap.address", zeroEnv.getHostname());
    xmlConfig.setProperty("communication.remotemap.local-port", "1101");
    xmlConfig.setProperty("communication.remotemap.server-net-address", zeroEnv.getHostname());
    xmlConfig.setProperty("communication.remotemap.server-port", "1101");

    xmlConfig.setProperty("rest-api.enabled", true);
    xmlConfig.setProperty("rest-api.server-config.host", "http://0.0.0.0");

    int restPort = restInitialPort_ + env.getId();
    xmlConfig.setProperty(XML_REST_PORT_FIELD, restPort);
    env.setProperty(XML_REST_PORT_FIELD, restPort);

    xmlConfig.setProperty("bdb-peer.holder-comm-address", createUID(zeroEnv.getId()));
    return xmlConfig;
  }
}
