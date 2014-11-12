package org.nebulostore.async.synchronization;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.async.AsyncMessagesContext;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.dispatcher.JobInitMessage;

/**
 * Module that synchronizes asynchronous messages data with other synchro-peers from each
 * synchro-peer group current instance belongs to.
 *
 * @author Piotr Malicki
 *
 */
public class SynchronizeAsynchronousMessagesModule extends JobModule {

  private static final Logger LOGGER = Logger
      .getLogger(SynchronizeAsynchronousMessagesModule.class);

  private final SynchroVisitor visitor_ = new SynchroVisitor();
  private CommAddress myAddress_;
  private AsyncMessagesContext context_;

  @Inject
  public void setDependencies(CommAddress myAddress, AsyncMessagesContext context) {
    myAddress_ = myAddress;
    context_ = context;
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected class SynchroVisitor extends MessageVisitor<Void> {

    public Void visit(JobInitMessage msg) {
      LOGGER.info("Starting synchronization of asynchronous messages.");
      if (context_.isInitialized()) {
        jobId_ = msg.getId();
        outQueue_.add(new JobInitMessage(new RetrieveAsynchronousMessagesModule(context_
            .getSynchroGroupForPeerCopy(myAddress_), myAddress_)));
        LOGGER.debug("Started retrieve module for our synchro-group: " +
            context_.getSynchroGroupForPeerCopy(myAddress_));

        for (final CommAddress peer : context_.getRecipientsData().getRecipients()) {
          Set<CommAddress> synchroGroup = context_.getSynchroGroupForPeerCopy(peer);
          if (synchroGroup == null) {
            LOGGER.info("Cannot get synchro group of peer " + peer + " from cache.");
          } else {
            outQueue_.add(new JobInitMessage(new RetrieveAsynchronousMessagesModule(Sets
                .newHashSet(synchroGroup), peer)));
          }
        }
      } else {
        LOGGER.warn("Async messages context has not yet been initialized, ending the module");
      }
      endJobModule();
      return null;
    }
  }

}
