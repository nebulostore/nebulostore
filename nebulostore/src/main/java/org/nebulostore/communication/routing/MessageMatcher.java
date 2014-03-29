package org.nebulostore.communication.routing;

import org.nebulostore.communication.messages.CommMessage;

/**
 * This object matches {@link CommMessage} to some conditions.
 *
 * @author Grzegorz Milka
 */
public interface MessageMatcher {
  boolean matchMessage(CommMessage msg);
}
