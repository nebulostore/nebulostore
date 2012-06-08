package org.nebulostore.communication.jxta;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

import net.jxta.document.AdvertisementFactory;
import net.jxta.endpoint.StringMessageElement;
import net.jxta.id.ID;
import net.jxta.impl.pipe.BlockingWireOutputPipe;
import net.jxta.impl.util.pipe.reliable.OutgoingPipeAdaptorSync;
import net.jxta.peer.PeerID;
import net.jxta.peergroup.PeerGroup;
import net.jxta.pipe.OutputPipe;
import net.jxta.pipe.PipeService;
import net.jxta.protocol.PipeAdvertisement;

import org.apache.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;
import org.nebulostore.appcore.Message;
import org.nebulostore.appcore.Module;
import org.nebulostore.communication.exceptions.CommException;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.ErrorCommMessage;

/**
 * Module responsible for sending data over JXTA Network.
 * 
 * @author Marcin Walas
 */
public class MessengerService extends Module {

  private static Logger logger_ = Logger.getLogger(MessengerService.class);

  private static final String MESSAGE_PIPE_ID_STR = "urn:jxta:uuid-"
      + "59616261646162614E504720503250338944BCED387C4A2BBD8E9411B78C284104";
  private static final int MAX_RETRIES = 3;
  private static final int PIPE_TIMEOUT = 3000;

  private final PeerGroup peerGroup_;
  private final BlockingQueue<Message> outQueue_;

  private final Map<String, OutgoingPipeAdaptorSync> pipes_;

  private int counter_ = 0;
  private int lastCounter_ = 0;
  private int byteCounter_ = 0;

  private long lastStatsRun_;
  private long lastSeenBytes_ = 0;

  public MessengerService(BlockingQueue<Message> inQueue,
      BlockingQueue<Message> outQueue, PeerGroup peerGroup) {
    super(inQueue, outQueue);
    this.outQueue_ = outQueue;

    peerGroup_ = peerGroup;

    pipes_ = new HashMap<String, OutgoingPipeAdaptorSync>(256);

    logger_.info("fully initialised");

    lastStatsRun_ = System.currentTimeMillis();
    (new Timer()).schedule(new StatsTimer(), 10000, 10000);
  }

  class StatsTimer extends TimerTask {

    @Override
    public void run() {
      long now = System.currentTimeMillis();
      logger_.info("STATS: bytes per second: " +
          ((byteCounter_ - lastSeenBytes_) * 1000 / (now - lastStatsRun_)) +
          " messages per second: " +
          ((counter_ - lastCounter_) * 1000 / (now - lastStatsRun_)));
      lastStatsRun_ = now;
      lastSeenBytes_ = byteCounter_;
      lastCounter_ = counter_;
    }
  }

  public static PipeAdvertisement getPipeAdvertisement() {
    PipeAdvertisement advertisement = (PipeAdvertisement) AdvertisementFactory
        .newAdvertisement(PipeAdvertisement.getAdvertisementType());

    advertisement.setPipeID(ID.create(URI.create(MESSAGE_PIPE_ID_STR)));
    advertisement.setType(PipeService.UnicastType);
    advertisement.setName("Nebulostore messaging");
    return advertisement;
  }

  private void oldProcessMessage(Message msg) {
    if (((CommMessage) msg).getDestinationAddress() == null) {
      outQueue_.add(new ErrorCommMessage((CommMessage) msg, new CommException(
          "Message " + msg.toString() + " with null destination address")));
      logger_.error("Message with null destination address");
      return;
    }

    PeerID destAddress = ((CommMessage) msg).getDestinationAddress()
        .getPeerId();

    OutputPipe pipe = new BlockingWireOutputPipe(peerGroup_,
        MessengerService.getPipeAdvertisement(), destAddress);

    try {
      pipe.send(wrapMessage(msg));
    } catch (IOException e) {
      logger_.error(e);
      outQueue_.add(new ErrorCommMessage((CommMessage) msg, e));
    }
  }

  @Override
  protected void processMessage(Message msg) {

    try {
      processMessageRetry(msg, 0, null);
    } catch (Throwable t) {
      logger_.error("Serious error. Sleeping for 5 sec...", t);
      t.printStackTrace();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

  }

  private void processMessageRetry(Message msg, int retries, Exception lastError) {
    if (retries < MAX_RETRIES) {
      if (((CommMessage) msg).getDestinationAddress() == null) {
        outQueue_.add(new ErrorCommMessage((CommMessage) msg,
            new CommException("Message " + msg.toString() +
                " with null destination address")));
        logger_.error("Message with null destination address");
        return;
      }

      PeerID destAddress = ((CommMessage) msg).getDestinationAddress()
          .getPeerId();

      try {
        logger_.debug("Message to be sent over network to: " + destAddress);

        if (!pipes_.containsKey(destAddress.toString())) {
          logger_.debug("Refreshing pipe.. for address: " +
              destAddress.toString());
          refreshPipe(destAddress);
          logger_.debug("Refreshing pipe. Finished");
        }
        pipes_.get(destAddress.toString()).send(wrapMessage(msg));
        logger_.debug("sent to " + destAddress);

      } catch (IOException e) {
        logger_.error(e);
        pipes_.get(destAddress.toString()).close();
        try {
          refreshPipe(destAddress);
        } catch (IOException ex) {
          logger_.error(ex);
        }

        processMessageRetry(msg, retries + 1, e);
      }
    } else {
      outQueue_.add(new ErrorCommMessage((CommMessage) msg, lastError));
      logger_.error("Max retries elapsed, raising error message...");
    }

  }

  private void refreshPipe(PeerID destAddress) throws IOException {
    OutputPipe pipe = new BlockingWireOutputPipe(peerGroup_,
        getPipeAdvertisement(), destAddress);
    OutgoingPipeAdaptorSync pipeAdaptor = new OutgoingPipeAdaptorSync(pipe);
    pipeAdaptor.setTimeout(PIPE_TIMEOUT);
    pipes_.put(destAddress.toString(), pipeAdaptor);
  }

  private net.jxta.endpoint.Message wrapMessage(Message msg) {
    net.jxta.endpoint.Message jxtaMessage = new net.jxta.endpoint.Message();
    String serialized = new String();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    try {
      oos = new ObjectOutputStream(baos);
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    try {
      oos.writeObject(msg);
    } catch (IOException e) {
      e.printStackTrace();
    }
    serialized = new String(Base64.encode(baos.toByteArray()));

    byteCounter_ += serialized.length();
    counter_++;

    jxtaMessage.addMessageElement(new StringMessageElement("serialized",
        serialized, null));
    return jxtaMessage;
  }

}
