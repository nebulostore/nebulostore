package org.nebulostore.async.peerselection;

/**
 * Base interface for factories of synchro-peer selection modules.
 *
 * @author Piotr Malicki
 *
 */
public interface SynchroPeerSelectionModuleFactory {

  /**
   * Create new module.
   *
   * @return Module that will decide if last peer from network context should be added as
   * synchro-peer of current instance.
   */
  SynchroPeerSelectionModule createModule();

}
