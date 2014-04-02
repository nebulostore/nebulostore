package org.nebulostore.communication.routing;

import java.math.BigInteger;

/**
 * Identifier used to differentiate different send operations.
 *
 * @author Grzegorz Milka
 */
public class SendOperationIdentifier {
  private final BigInteger id_;

  public SendOperationIdentifier(BigInteger id) {
    id_ = id;
  }

  public BigInteger getId() {
    return id_;
  }
}
