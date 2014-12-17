package org.nebulostore.rest;

/**
 * @author lukaszsiczek
 */
public interface RestModule extends Runnable {
  void shutDown();
}
