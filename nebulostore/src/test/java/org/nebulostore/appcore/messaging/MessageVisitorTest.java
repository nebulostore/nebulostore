package org.nebulostore.appcore.messaging;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.visitors.TestVisitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * @author Bolek Kulbabinski
 */
public class MessageVisitorTest {

  @Test
  public void testHandleSpecificMessageType() throws NebuloException {
    Message message = new AMessage();
    AtomicInteger counter = new AtomicInteger(0);
    MessageVisitor visitor = TestVisitors.getAVisitor(counter);

    message.accept(visitor);

    assertEquals("Handler call counter should be equal to one.", 1, counter.get());
  }

  @Test
  public void testHandleAllMessages() throws NebuloException {
    Message message = new AMessage();
    AtomicInteger counter = new AtomicInteger(0);
    MessageVisitor visitor = TestVisitors.getSubclassedVisitor(counter);

    message.accept(visitor);

    assertEquals("Handler call counter should be equal to one.", 1, counter.get());
  }

  @Test(expected = UnsupportedMessageException.class)
  public void testHandleUnsupportedMessageType() throws NebuloException {
    Message message = new AMessage();
    MessageVisitor visitor = TestVisitors.getEmptyVisitor();

    // Should throw UnsupportedMessageException.
    message.accept(visitor);
  }

  @Test(expected = UnsupportedMessageException.class)
  public void testHandleNotMessageSubclass() throws NebuloException {
    MessageVisitor visitor = TestVisitors.getMVisitor(null);

    // Should throw UnsupportedMessageException.
    visitor.visit(new Integer(123));
  }

  @Test
  public void testExceptionFromVisitMethod() throws NebuloException {
    Message message = new AMessage();
    MessageVisitor visitor = TestVisitors.getThrowingVisitor();

    try {
      message.accept(visitor);
      fail("ThrowingVisitor handler should throw exception.");
    } catch (NebuloException e) {
      assertNotNull("Exception should contain the real cause.", e.getCause());
      assertEquals("Cause message should be correct.",
          TestVisitors.EXCEPTION_MSG, e.getCause().getMessage());
    }
  }
}
