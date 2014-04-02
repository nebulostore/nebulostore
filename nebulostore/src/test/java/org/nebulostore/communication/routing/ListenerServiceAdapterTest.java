package org.nebulostore.communication.routing;

import java.io.IOException;
import java.util.Observer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.SerializationUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.StubCommMessage;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ListenerServiceAdapterTest {
  private static final CommAddress SOURCE = new CommAddress(0, 0);
  private static final CommAddress DEST = new CommAddress(0, 1);
  private ListenerService listenerService_;

  private ByteListenerService byteListener_;
  private BlockingQueue<byte[]> byteQueue_;
  private BlockingQueue<CommMessage> listeningQueue_;

  @Before
  public void setUp() throws IOException {
    listeningQueue_ = new LinkedBlockingQueue<CommMessage>();
    byteQueue_ = new LinkedBlockingQueue<byte[]>();
    byteListener_ = mock(ByteListenerService.class);
    when(byteListener_.getListeningQueue()).thenReturn(byteQueue_);
    listenerService_ = new ListenerServiceAdapter(byteListener_, listeningQueue_);
    listenerService_.startUp();
  }

  @After
  public void tearDown() throws InterruptedException {
    listenerService_.shutDown();
    verify(byteListener_).deleteObserver((Observer) any());
  }

  @Test
  public void hasAddedAnObserver() throws Exception {
    verify(byteListener_).addObserver((Observer) any());
  }

  @Test
  public void shouldReceiveMessageCorrectly() throws Exception {
    ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
    verify(byteListener_).addObserver(argument.capture());
    Observer observer = argument.getValue();
    CommMessage msg = new StubCommMessage(SOURCE, DEST);
    byte[] serializedMsg = SerializationUtils.serialize(msg);
    byteQueue_.add(serializedMsg);
    observer.update(null, null);
    assertEquals(msg, listeningQueue_.take());
  }

  @Test
  public void shouldStillWorkAfterReceivingFaultyMessage() throws Exception {
    byte[] faultyMsg = {1};
    ArgumentCaptor<Observer> argument = ArgumentCaptor.forClass(Observer.class);
    verify(byteListener_).addObserver(argument.capture());
    Observer observer = argument.getValue();
    byteQueue_.add(faultyMsg);
    observer.update(null, null);
    shouldReceiveMessageCorrectly();
  }
}
