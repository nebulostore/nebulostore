package org.nebulostore.communication.bootstrap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collection;

/**
 * Client which connects to {@link BootstrapServer}s for information.
 *
 * @author Grzegorz Milka
 *
 */
public class BootstrapClient implements BootstrapService {
  private final Collection<InetSocketAddress> bootstrapNetAddresses_;
  private BootstrapInformation bootstrapInformation_;

  public BootstrapClient(Collection<InetSocketAddress> bootstrapNetAddresses) {
    bootstrapNetAddresses_ = bootstrapNetAddresses;
  }

  @Override
  public BootstrapInformation getBootstrapInformation() {
    return bootstrapInformation_;
  }

  @Override
  public void shutDown() throws InterruptedException {
  }

  @Override
  public synchronized void startUp() throws IOException {
    for (InetSocketAddress bootstrapNetAddress: bootstrapNetAddresses_) {
      try {
        bootstrapInformation_ = connectAndGetInformation(bootstrapNetAddress);
        if (bootstrapInformation_ != null) {
          break;
        }
      } catch (IOException e) {
        continue;
      }
    }
    if (bootstrapInformation_ == null) {
      throw new IOException("Could not get correct bootstrap information from servers.");
    }
  }

  private BootstrapInformation connectAndGetInformation(InetSocketAddress bootstrapNetAddress)
      throws IOException {
    BootstrapInformation bootInfo;
    try (Socket socket = new Socket(
        bootstrapNetAddress.getAddress(), bootstrapNetAddress.getPort())) {
      ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
      try {
        bootInfo = (BootstrapInformation) ois.readObject();
      } catch (ClassNotFoundException e) {
        bootInfo = null;
      }
      ois.close();
    }
    return bootInfo;
  }
}
