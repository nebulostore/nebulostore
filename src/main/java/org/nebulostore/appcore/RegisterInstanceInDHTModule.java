package org.nebulostore.appcore;

import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.nebulostore.api.ApiFacade;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.communication.CommunicationPeer;
import org.nebulostore.communication.dht.ValueDHT;
import org.nebulostore.communication.messages.dht.ErrorDHTMessage;
import org.nebulostore.communication.messages.dht.GetDHTMessage;
import org.nebulostore.communication.messages.dht.OkDHTMessage;
import org.nebulostore.communication.messages.dht.PutDHTMessage;
import org.nebulostore.communication.messages.dht.ValueDHTMessage;
import org.nebulostore.dispatcher.messages.JobInitMessage;

//TODO(szm): Maybe it should be a ReturningJobModule??

/**
 * Module checks if InstaceMetadata is already in DHT. If not tries to
 * load it from disk(todo) and if it's not there, puts empty InstanceMetadata in DHT.
 * @author szymonmatejczyk
 *
 */
public class RegisterInstanceInDHTModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RegisterInstanceInDHTModule.class);

  private final MessageVisitor<Void> visitor_ = new RIIDHTVisitor();

  /**
   * Visitor state.
   */
  private enum State { QUERY_DHT, WAITING_FOR_RESPONSE, PUT_DHT }


  /**
   * Visitor.
   */
  public class RIIDHTVisitor extends MessageVisitor<Void> {
    State state_ = State.QUERY_DHT;
    @Override
    public Void visit(JobInitMessage message) {
      jobId_ = message.getId();
      logger_.debug("Trying to retrive InstanceMetadata from DHT taskId: " + jobId_);
      networkQueue_.add(new GetDHTMessage(jobId_,
          GlobalContext.getInstance().getInstanceID().toKeyDHT()));
      state_ = State.WAITING_FOR_RESPONSE;
      return null;
    }

    @Override
    public Void visit(ErrorDHTMessage message) {
      if (state_ == State.WAITING_FOR_RESPONSE) {
        logger_.debug("Unable to retrive InstanceMetadata from DHT, putting new.");
        // TODO(szm): read from file if exists
        networkQueue_.add(new PutDHTMessage(jobId_,
            GlobalContext.getInstance().getInstanceID().toKeyDHT(),
            new ValueDHT(new InstanceMetadata(ApiFacade.getAppKey(),
                CommunicationPeer.getPeerAddress(), new LinkedList<InstanceID>()))));
        state_ = State.PUT_DHT;
      } else if (state_ == State.PUT_DHT) {
        logger_.error("Unable to put InstanceMetadata to DHT. " +
            message.getException().getMessage());
        endJobModule();
      } else {
        logger_.warn("Received unexpected ErrorDHTMessage.");
      }
      return null;
    }
    @Override
    public Void visit(ValueDHTMessage message) {
      if (state_ == State.WAITING_FOR_RESPONSE) {
        logger_.debug("InstanceMetadata already in DHT, nothing to do.");
        endJobModule();
      } else {
        logger_.warn("Received unexpected ValueDHTMessage");
      }
      return null;
    }

    @Override
    public Void visit(OkDHTMessage message) {
      if (state_ == State.PUT_DHT) {
        logger_.debug("Successfuly put InstanceMetadata into DHT.");
        endJobModule();
      } else {
        logger_.warn("Received unexpected OkDHTMessage.");
      }
      return null;
    }

  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

}