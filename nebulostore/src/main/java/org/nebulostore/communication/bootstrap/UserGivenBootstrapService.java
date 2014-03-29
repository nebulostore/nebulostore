package org.nebulostore.communication.bootstrap;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.communication.naming.CommAddress;


/**
 * @author Grzegorz Milka
 */
public class UserGivenBootstrapService implements BootstrapService {
  private final BootstrapInformation bootInfo_;

  @Inject
  public UserGivenBootstrapService(
      @Named("communication.bootstrap-comm-address") CommAddress commAddress) {
    Collection<CommAddress> bootAddresses = new LinkedList<>();
    bootAddresses.add(commAddress);
    bootInfo_ = new BootstrapInformation(bootAddresses);
  }


  @Override
  public BootstrapInformation getBootstrapInformation() {
    return bootInfo_;
  }

  @Override
  public void startUp() throws IOException {
  }

  @Override
  public void shutDown() throws InterruptedException {
  }


}
