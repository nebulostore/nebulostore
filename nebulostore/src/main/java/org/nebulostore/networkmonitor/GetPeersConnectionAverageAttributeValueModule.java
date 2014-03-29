package org.nebulostore.networkmonitor;

import com.google.common.base.Predicate;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Returns average connectionAttribute retrived from DHT for a given peer.
 * @author szymonmatejczyk
 */
public class GetPeersConnectionAverageAttributeValueModule
  extends GetPeersConnectionAverageStatisticModule {
  public GetPeersConnectionAverageAttributeValueModule(CommAddress peer,
      final ConnectionAttribute connectionAttribute) {
    super(peer, new Predicate<PeerConnectionSurvey>() {
      @Override
      public boolean apply(PeerConnectionSurvey pcs) {
        return pcs.getAttribute() == connectionAttribute;
      }
    });
  }
}
