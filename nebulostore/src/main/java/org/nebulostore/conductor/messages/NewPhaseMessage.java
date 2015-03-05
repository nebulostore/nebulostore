package org.nebulostore.conductor.messages;

import org.nebulostore.appcore.messaging.Message;

/**
 * Message send by TestModule to itself when test advances to next phase.
 * @author szymonmatejczyk
 *
 */
public class NewPhaseMessage extends Message {
  private static final long serialVersionUID = -1962731068882917843L;

  public NewPhaseMessage() {
    super();
  }

}
