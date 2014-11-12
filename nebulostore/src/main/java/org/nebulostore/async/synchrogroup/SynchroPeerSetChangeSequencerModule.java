package org.nebulostore.async.synchrogroup;

import java.util.Set;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.appcore.modules.ReturningJobModule;
import org.nebulostore.async.synchrogroup.messages.LastFoundPeerMessage;
import org.nebulostore.async.synchrogroup.selector.SynchroPeerSelector;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.utils.Pair;

/**
 * Module that run all request to change synchro-peer set of current instance sequentially
 * in the order of receiving them.
 *
 * @author Piotr Malicki
 *
 */
public class SynchroPeerSetChangeSequencerModule extends JobModule {

  private static Logger logger_ = Logger.getLogger(SynchroPeerSetChangeSequencerModule.class);
  private static final int CHANGE_SYNCHRO_PEER_SET_MODULE_TIMEOUT = 5000;

  private final MessageVisitor<Void> visitor_ = new SPSUSequencerVisitor();
  private final SynchroPeerSelector selector_;

  @Inject
  public SynchroPeerSetChangeSequencerModule(SynchroPeerSelector selector) {
    selector_ = selector;
  }

  protected class SPSUSequencerVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage message) {
      return null;
    }

    public Void visit(LastFoundPeerMessage message) {
      if (message.getLastPeer() != null) {
        Pair<Set<CommAddress>, Set<CommAddress>> synchroGroupChange =
            selector_.decide(message.getLastPeer());
        if (synchroGroupChange != null &&
            (synchroGroupChange.getFirst() != null || synchroGroupChange.getSecond() != null)) {
          logger_
              .debug("Received next request to change synchro group with synchro peers to add: " +
                  synchroGroupChange.getFirst() + " and synchro peers to remove: " +
                  synchroGroupChange.getSecond());
          ReturningJobModule<Void> module =
              new ChangeSynchroPeerSetModule(synchroGroupChange.getFirst(),
                  synchroGroupChange.getSecond());
          module.setOutQueue(outQueue_);
          module.runThroughDispatcher();
          try {
            module.getResult(CHANGE_SYNCHRO_PEER_SET_MODULE_TIMEOUT);
          } catch (NebuloException e) {
            logger_.warn(module.getClass() + " failed");
          }
        }
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}
