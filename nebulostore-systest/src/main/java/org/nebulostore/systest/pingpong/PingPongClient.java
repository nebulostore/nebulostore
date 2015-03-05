package org.nebulostore.systest.pingpong;

import java.math.BigInteger;
import java.util.List;

import org.apache.log4j.Logger;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.conductor.messages.UserCommMessage;
import org.nebulostore.crypto.CryptoUtils;

/**
 * ping-pong test implementation.
 * Nodes create a perfect binary tree with height = 3
 * and send ping-pong messages from root to leaves and back.
 * 1st phase: A sends Ping to B, C (notation: A->B,C)
 * 2nd phase: B->D,E; C->F,G
 * 3rd phase: D->H,I; E->J,K; F->L,M; G->N,O
 * 4th phase: H, I send Pong to D (notation D<-H,I); E<-J,K; F<-L,M; G<-N,O
 * 5th phase: B<-D,E; C<-F,G
 * 6th phase: A<-B,C
 * @author szymonmatejczyk, lukaszsiczek
 */
public final class PingPongClient extends ConductorClient {
  private static final long serialVersionUID = 8676871234510749533L;
  private static Logger logger_ = Logger.getLogger(PingPongClient.class);

  private CommAddress parentAddress_;
  private BigInteger randomNumber_;
  private final List<CommAddress> childrenAddresses_;
  private Integer receivedPongs_ = 0;
  private final int id_;

  public PingPongClient(String serverJobId, CommAddress serverAddress, int numPhases,
          List<CommAddress> childrenPongAddress, int id) {
    super(serverJobId, numPhases, serverAddress);
    childrenAddresses_ = childrenPongAddress;
    id_ = id;
    if (id_ == 0) {
      randomNumber_ = CryptoUtils.getRandomId();
    }
  }

  @Override
  protected void initVisitors() {
    visitors_ =  new TestingModuleVisitor[numPhases_ + 2];
    visitors_[0] = new EmptyInitializationVisitor();
    if (id_ == 0) {
      visitors_[1] = new VisitorSendPing();
      for (int i = 2; i <= 5; ++i) {
        visitors_[i] = new EmptyVisitor();
      }
      visitors_[6] = new VisitorReceivedPong();
    } else if (id_ >= 1 && id_ <= 2) {
      visitors_[1] = new VisitorReceivedPing();
      visitors_[2] = new VisitorSendPing();
      for (int i = 3; i <= 4; ++i) {
        visitors_[i] = new EmptyVisitor();
      }
      visitors_[5] = new VisitorReceivedPong();
      visitors_[6] = new VisitorSendPong();
    } else if (id_ >= 3 && id_ <= 6) {
      visitors_[1] = new EmptyVisitor();
      visitors_[2] = new VisitorReceivedPing();
      visitors_[3] = new VisitorSendPing();
      visitors_[4] = new VisitorReceivedPong();
      visitors_[5] = new VisitorSendPong();
      visitors_[6] = new EmptyVisitor();
    } else if (id_ >= 7 && id_ <= 14) {
      for (int i = 1; i <= 2; ++i) {
        visitors_[i] = new EmptyVisitor();
      }
      visitors_[3] = new VisitorReceivedPing();
      visitors_[4] = new VisitorSendPong();
      for (int i = 5; i <= 6; ++i) {
        visitors_[i] = new EmptyVisitor();
      }
    }
    visitors_[7] = new IgnoreNewPhaseVisitor();
  }

  /**
   *
   *
   * Phase - send Ping to children.
   * @author lukaszsiczek
   */
  protected final class VisitorSendPing extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      for (CommAddress address : childrenAddresses_) {
        if (sendMessage(new UserCommMessage(jobId_,
          address, randomNumber_.add(BigInteger.ONE), phase_))) {
          logger_.debug("Send PingMessage to " + address.toString());
        }
      }
      phaseFinished();
    }
  }

  /**
   * Phase - received Ping from parent.
   * @author lukaszsiczek
   */
  protected final class VisitorReceivedPing extends IgnoreNewPhaseVisitor {
    @Override
    public void visit(UserCommMessage message) {
      logger_.debug("Received PingMessage from parent.");
      randomNumber_ = (BigInteger) message.getContent();
      parentAddress_ = message.getSourceAddress();
      phaseFinished();
    }
  }

  /**
   * Phase - send Pong to parent.
   * @author lukaszsiczek
   */
  protected final class VisitorSendPong extends TestingModuleVisitor {

    @Override
    public void visit(NewPhaseMessage message) {
      logger_.debug("Send PongMessage to parent: " + parentAddress_.toString());
      sendMessage(new UserCommMessage(jobId_, parentAddress_, randomNumber_, phase_));
      phaseFinished();
    }

  }

  /**
   * Phase - received Pong from children.
   * @author lukaszsiczek
   */
  protected final class VisitorReceivedPong extends IgnoreNewPhaseVisitor {
    @Override
    public void visit(UserCommMessage message) {
      BigInteger received = (BigInteger) message.getContent();
      logger_.debug("Received PongMessage from child: " + message.getSourceAddress().toString());
      assertTrue(received.equals(randomNumber_.add(BigInteger.ONE)), "Correct number received.");
      synchronized (receivedPongs_) {
        ++receivedPongs_;
        if (receivedPongs_ == childrenAddresses_.size()) {
          phaseFinished();
        }
      }
    }
  }
}
