package org.nebulostore.systest.basictest;

import java.math.BigInteger;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.UserCommMessage;

/**
 * Pong.
 * @author szymonmatejczyk
 */
public final class PongClient extends ConductorClient {
  private static Logger logger_ = Logger.getLogger(PongClient.class);
  private static final long serialVersionUID = -7238750658102427676L;

  BigInteger magicNumber_;
  CommAddress sender_;

  public PongClient(String serverJobId, CommAddress serverAddress, int numPhases) {
    super(serverJobId, numPhases, serverAddress);
  }

  @Override
  protected void initVisitors() {
    visitors_ =  new TestingModuleVisitor[numPhases_ + 2];
    visitors_[0] = new EmptyInitializationVisitor();
    visitors_[1] = new Visitor1();
    visitors_[2] = new Visitor2();
    visitors_[3] = new IgnoreNewPhaseVisitor();
  }

  /**
   * Phase 1 - receive Ping.
   */
  protected final class Visitor1 extends IgnoreNewPhaseVisitor {
    @Override
    public void visit(UserCommMessage message) {
      logger_.debug("Received PingMessage.");
      magicNumber_ = (BigInteger) message.getContent();
      sender_ = message.getSourceAddress();
      phaseFinished();
    }
  }

  /**
   * Phase 2 - send Pong.
   */
  protected final class Visitor2 extends TestingModuleVisitor {
    @Override
    public void visit(NewPhaseMessage message) {
      networkQueue_.add(new UserCommMessage(jobId_, sender_, magicNumber_.add(BigInteger.ONE),
          phase_));
      phaseFinished();
    }
  }
}
