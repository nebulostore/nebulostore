package org.nebulostore.communication.netutils.remotemap;

import java.io.Serializable;

/**
 * @author Grzegorz Milka
 */
public interface Transaction extends Serializable {
  Serializable performTransaction(int type, Serializable key, Serializable value);
}
