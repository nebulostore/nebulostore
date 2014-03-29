package org.nebulostore.dht.exceptions;

/**
 * @author Marcin Walas
 */
public class ValueNotFound extends Exception {
  private static final long serialVersionUID = -4699767558431415336L;

  public ValueNotFound(String msg) {
    super(msg);
  }
}
