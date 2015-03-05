package org.nebulostore.systest.basictest;

import java.math.BigInteger;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.UserCommMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Simple ping-pong test implementation.
 * @author szymonmatejczyk
 */
public final class PingClient extends ConductorClient {
  private static final long serialVersionUID = 8676871234510749533L;
  private static Logger logger_ = Logger.getLogger(PingClient.class);

  private final CommAddress pongAddress_;
  private final BigInteger magicNumber_;

  public PingClient(String serverJobId, CommAddress serverAddress, int numPhases,
      CommAddress pongAddress) {
    super(serverJobId, numPhases, serverAddress);
    magicNumber_ = CryptoUtils.getRandomId();
    pongAddress_ = pongAddress;
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
   * Phase 1 - send Ping.
   * @author szymonmatejczyk
   */
  protected final class Visitor1 extends TestingModuleVisitor {
    public void visit(NewPhaseMessage message) {
      networkQueue_.add(new UserCommMessage(jobId_, pongAddress_, magicNumber_, phase_));
      phaseFinished();
    }
  }

  /**
   * Phase 2 - receive Pong.
   * @author szymonmatejczyk
   */
  protected final class Visitor2 extends IgnoreNewPhaseVisitor {
    @Override
    public void visit(UserCommMessage message) {
      BigInteger received = (BigInteger) message.getContent();
      logger_.debug("Received PongMessage.");
      assertTrue(received.equals(magicNumber_.add(BigInteger.ONE)), "Correct number received.");
      phaseFinished();
    }
  }
}
