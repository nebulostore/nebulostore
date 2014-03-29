package org.nebulostore.communication.routing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.StubCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.naming.CommAddressResolver;
import org.nebulostore.communication.routing.SendResult.ResultType;
import org.nebulostore.utils.BlockingAnswer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Grzegorz Milka
 */
public final class MessageSenderTest {
  private static final CommAddress SOURCE = new CommAddress(0, 0);
  private static final CommAddress DEST = new CommAddress(0, 1);
  private static final String MSG = "TEST MESSAGE";
  private static final CommMessage COMM_MSG = new StubCommMessage(SOURCE, DEST, MSG);
  private MessageSender sender_;

  private ExecutorService executor_;
  private OOSDispatcher dispatcher_;
  private CommAddressResolver resolver_;

  @Before
  public void setUp() {
    executor_ = Executors.newCachedThreadPool();
    dispatcher_ = mock(OOSDispatcher.class);
    resolver_ = mock(CommAddressResolver.class);
    sender_ = new MessageSender(executor_, dispatcher_, resolver_);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void shouldAddErrorSendResultToQueueWhenMessageSendStreamThrowsException()
      throws Exception {
    OutputStream os = mock(OutputStream.class);
    ObjectOutputStream oos = new ObjectOutputStream(os);
    InetSocketAddress destAddress = new InetSocketAddress(0);
    when(dispatcher_.getStream(eq(destAddress))).thenReturn(oos);
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);
    doThrow(new IOException()).when(os).write(any(byte[].class), anyInt(), anyInt());
    BlockingQueue<SendResult> resultQueue = new LinkedBlockingQueue<SendResult>();
    sender_.sendMessage(COMM_MSG, resultQueue);
    SendResult result = resultQueue.take();
    assertEquals(ResultType.ERROR, result.getType());
    assertEquals(COMM_MSG, result.getMessage());
    sender_.shutDown();
    verify(dispatcher_).putStream(eq(destAddress));
    verify(resolver_).reportFailure(DEST);
  }

  @Test
  public void shouldAddOkSendResultWhenSendMessageCorrectly() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    InetSocketAddress destAddress = new InetSocketAddress(0);
    when(dispatcher_.getStream(eq(destAddress))).thenReturn(oos);
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);
    BlockingQueue<SendResult> resultQueue = new LinkedBlockingQueue<SendResult>();
    sender_.sendMessage(COMM_MSG, resultQueue);
    SendResult result = resultQueue.take();
    assertEquals(ResultType.OK, result.getType());
    assertEquals(COMM_MSG, result.getMessage());

    sender_.shutDown();
    verify(dispatcher_).putStream(eq(destAddress));
    assertTrue(checkIfMessageIsEqual(baos.toByteArray(), COMM_MSG));
  }

  @Test
  public void shouldReturnExceptionWhenMessageSendIsInterrupted() throws Exception {
    InetSocketAddress destAddress = new InetSocketAddress(0);
    BlockingAnswer<ObjectOutputStream> blockingAnswer = new BlockingAnswer<>();

    when(dispatcher_.getStream(eq(destAddress))).thenAnswer(blockingAnswer);
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);
    Future<?> future = sender_.sendMessage(COMM_MSG);
    blockingAnswer.waitForCall();
    future.cancel(true);
    try {
      future.get();
      fail("Expected CancellationException");
    } catch (CancellationException e) {
      assertTrue(true);
    } catch (ExecutionException | InterruptedException t) {
      fail("Expected CancellationException");
    }

    executor_.shutdown();
    assertTrue(executor_.isShutdown());
    verify(resolver_, never()).reportFailure(DEST);
    sender_.shutDown();
  }

  @Test
  public void shouldReturnExceptionWhenMessageSendStreamThrowsException() throws Exception {
    OutputStream os = mock(OutputStream.class);
    ObjectOutputStream oos = new ObjectOutputStream(os);
    InetSocketAddress destAddress = new InetSocketAddress(0);
    when(dispatcher_.getStream(eq(destAddress))).thenReturn(oos);
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);
    doThrow(new IOException()).when(os).write(any(byte[].class), anyInt(), anyInt());
    Future<?> future = sender_.sendMessage(COMM_MSG);
    try {
      future.get();
      fail("Expected ExecutionException");
    } catch (ExecutionException e) {
      assertTrue("Throws IOException",
          e.getCause().getCause() instanceof IOException);
    }
    sender_.shutDown();
    verify(dispatcher_).putStream(eq(destAddress));
    verify(resolver_).reportFailure(DEST);
  }

  @Test
  public void shouldReturnExceptionWhenRecipientIsNotPresent() throws Exception {
    InetSocketAddress destAddress = new InetSocketAddress(0);
    when(dispatcher_.getStream(eq(destAddress))).thenThrow(new SocketException());
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);
    Future<?> future = sender_.sendMessage(COMM_MSG);
    try {
      future.get();
      fail("Should have thrown SocketException");
    } catch (ExecutionException e) {
      assertTrue("Throws SocketException",
          e.getCause().getCause() instanceof SocketException);
    }
    sender_.shutDown();
    verify(dispatcher_, never()).putStream(any(InetSocketAddress.class));
  }

  @Test
  public void shouldSendMessageCorrectly() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    InetSocketAddress destAddress = new InetSocketAddress(0);
    when(dispatcher_.getStream(eq(destAddress))).thenReturn(oos);
    when(resolver_.resolve(eq(DEST))).thenReturn(destAddress);

    Future<?> future = sender_.sendMessage(COMM_MSG);
    future.get();
    sender_.shutDown();
    verify(dispatcher_).putStream(eq(destAddress));
    assertTrue(checkIfMessageIsEqual(baos.toByteArray(), COMM_MSG));
  }

  private boolean checkIfMessageIsEqual(byte[] byteArray, CommMessage msg) throws Exception {
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray))) {
      return msg.equals(ois.readObject());
    }
  }
}
