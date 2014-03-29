package org.nebulostore.communication.routing;

import org.nebulostore.appcore.messaging.Message;

/**
 * Waits for arrival of specified message and performs action on it.
 *
 * Since usually this object is executed on main thread then the code should be fast.
 *
 * @author Grzegorz Milka
 *
 */
public interface MessageListener {
  void onMessageReceive(Message msg);
}
