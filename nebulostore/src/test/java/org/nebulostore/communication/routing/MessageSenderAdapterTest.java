package org.nebulostore.communication.routing;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.nebulostore.communication.messages.CommMessage;
import org.nebulostore.communication.messages.StubCommMessage;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.communication.naming.CommAddressResolver;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MessageSenderAdapterTest {
  private static final CommAddress SOURCE = new CommAddress(0, 0);
  private static final CommAddress DEST = new CommAddress(0, 1);
  private static final InetSocketAddress DEST_NET = new InetSocketAddress(1);
  private MessageSender messageSender_;

  private ByteSender byteSender_;
  private CommAddressResolver resolver_;
  @Before
  public void setUp() {
    ExecutorService executor = spy(Executors.newFixedThreadPool(1));
    byteSender_ = mock(ByteSender.class);
    resolver_ = mock(CommAddressResolver.class);
    messageSender_ = new MessageSenderAdapter(byteSender_, executor, resolver_);
    messageSender_.startUp();
  }

  @After
  public void tearDown() throws InterruptedException {
    messageSender_.shutDown();
  }

  @Test
  public void shouldReportErrorWhenCouldntSendMessage() throws Exception {
    CommMessage msg = new StubCommMessage(SOURCE, DEST);
    when(resolver_.resolve(eq(DEST))).thenReturn(DEST_NET);
    doThrow(new IOException()).when(byteSender_).sendMessageSynchronously(
        eq(DEST_NET), (byte[]) any());
    boolean caught = false;
    try {
      messageSender_.sendMessageSynchronously(msg);
    } catch (IOException e) {
      caught = true;
    }
    assertTrue(caught);

    verify(resolver_).reportFailure(eq(DEST));
    verify(byteSender_).sendMessageSynchronously(eq(DEST_NET), (byte[]) any());
  }

  @Test
  public void shouldSendSynchronousMessageCorrectly() throws Exception {
    CommMessage msg = new StubCommMessage(SOURCE, DEST);
    when(resolver_.resolve(eq(DEST))).thenReturn(DEST_NET);
    messageSender_.sendMessageSynchronously(msg);
    verify(resolver_).resolve(eq(DEST));
    verify(byteSender_).sendMessageSynchronously(eq(DEST_NET), (byte[]) any());
  }

  @Test
  public void shouldSendAsynchronousMessageCorrectly() throws Exception {
    CommMessage msg = new StubCommMessage(SOURCE, DEST);
    when(resolver_.resolve(eq(DEST))).thenReturn(DEST_NET);
    MessageSendFuture future = messageSender_.sendMessage(msg);
    future.get();
    verify(resolver_).resolve(eq(DEST));
    verify(byteSender_).sendMessageSynchronously(eq(DEST_NET), (byte[]) any());
  }

}
