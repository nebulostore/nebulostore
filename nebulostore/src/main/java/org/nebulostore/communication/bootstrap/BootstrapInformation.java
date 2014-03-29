package org.nebulostore.communication.bootstrap;

import java.io.Serializable;
import java.util.Collection;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Holder for bootstrap data.
 *
 * @author Grzegorz Milka
 *
 */
public class BootstrapInformation implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Collection<CommAddress> bootstrapCommAddresses_;

  public BootstrapInformation(Collection<CommAddress> bootstrapCommAddresses) {
    bootstrapCommAddresses_ = bootstrapCommAddresses;
  }

  public Collection<CommAddress> getBootstrapCommAddresses() {
    return bootstrapCommAddresses_;
  }
}
