package org.nebulostore.communication.routing;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.commons.lang.SerializationException;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.nebulostore.communication.messages.CommMessage;

public class ListenerServiceAdapter implements ListenerService {
  private static final Logger LOGGER = Logger.getLogger(ListenerServiceAdapter.class);
  private final ByteListenerService byteListener_;
  private final BlockingQueue<CommMessage> listeningQueue_;
  private final Observer observer_;
  private final AtomicBoolean isRunning_;

  @Inject
  public ListenerServiceAdapter(ByteListenerService byteListener,
      @Named("communication.routing.listening-queue") BlockingQueue<CommMessage> listeningQueue) {
    byteListener_ = byteListener;
    listeningQueue_ = listeningQueue;
    observer_ = new ListenerObserver();
    isRunning_ = new AtomicBoolean(true);
  }

  @Override
  public BlockingQueue<CommMessage> getListeningQueue() {
    return listeningQueue_;
  }

  @Override
  public void startUp() {
    synchronized (this) {
      isRunning_.set(true);
      byteListener_.addObserver(observer_);
    }
    observer_.update(null, null);
  }

  @Override
  public synchronized void shutDown() {
    byteListener_.deleteObserver(observer_);
    isRunning_.set(false);
  }

  private class ListenerObserver implements Observer {

    @Override
    public void update(Observable o, Object arg) {
      LOGGER.debug("ListenerObserver.update()");
      synchronized (ListenerServiceAdapter.this) {
        if (!isRunning_.get()) {
          return;
        }
        Collection<byte[]> content = new LinkedList<byte[]>();
        byteListener_.getListeningQueue().drainTo(content);
        for (byte[] msg: content) {
          try {
            CommMessage commMsg = deserialize(msg);
            LOGGER.debug("Received msg: " + commMsg);
            listeningQueue_.add(commMsg);
          } catch (ClassCastException | ClassNotFoundException | SerializationException e) {
            LOGGER.debug("ListenerServiceAdapter.update() -> received message of unkown type: " +
              msg, e);
          }
        }
      }
    }
  }

  private CommMessage deserialize(byte[] msg) throws ClassNotFoundException {
    return (CommMessage) SerializationUtils.deserialize(msg);
  }
}
