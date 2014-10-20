package org.nebulostore.appcore.model;

import org.junit.Test;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * Simple unit test for NebuloList.
 * @author Bolek Kulbabinski
 */
public final class NebuloListTest {
  // TODO write tests for new lists' API

  // private static final String APP_KEY = "22";
  // private static final String OBJECT_ID = "123";
  //
  // private final NebuloElement a_ = NebuloObjectUtils.getNewNebuloElement("1", "11");
  // private final NebuloElement b_ = NebuloObjectUtils.getNewNebuloElement("2", "22");
  // private final NebuloElement c_ = NebuloObjectUtils.getNewNebuloElement("3", "33");
  // private final NebuloElement d_ = NebuloObjectUtils.getNewNebuloElement("4", "44");

  @Test
  public void testAddingAndIterating() throws NebuloException {
    // NebuloList list = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
    // list.append(a_);
    // list.append(c_);
    // ListIterator iter = list.iterator();
    // iter.next();
    // list.add(iter, b_);
    //
    // NebuloElement[] elements = { a_, b_, c_ };
    // verifyListElements(list, elements);
  }

  // @Test
  // public void testRemoving() throws NebuloException {
  // NebuloList list = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
  // list.append(a_);
  // list.append(b_);
  // list.append(c_);
  // list.append(d_);
  //
  // ListIterator iter = list.iterator();
  // iter.next();
  // iter.remove();
  // iter.next();
  // iter.next();
  // iter.remove();
  //
  // NebuloElement[] elements = { b_, d_ };
  // verifyListElements(list, elements);
  // }
  //
  // @Test
  // public void testMergeOk() throws NebuloException {
  // // A - B (C removed)
  // NebuloList one = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
  // one.append(a_);
  // one.append(b_);
  // one.removedIds_.add(c_.elementId_);
  //
  // // D - A - C
  // NebuloList two = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
  // two.append(d_);
  // two.append(a_);
  // two.append(c_);
  //
  // one.mergeWith(two);
  //
  // assertEquals(one.elements_.size(), 3);
  // assertEquals(one.elements_.get(0), d_);
  // assertEquals(one.elements_.get(1), a_);
  // assertEquals(one.elements_.get(2), b_);
  // }
  //
  // @Test (expected = ListMergeException.class)
  // public void testMergeBad() throws NebuloException {
  // // A - B - C
  // NebuloList one = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
  // one.append(a_);
  // one.append(b_);
  // one.append(c_);
  //
  // // D - C - A - B
  // NebuloList two = NebuloObjectUtils.getNewNebuloList(APP_KEY, OBJECT_ID);
  // two.append(d_);
  // two.append(c_);
  // two.append(a_);
  // two.append(b_);
  //
  // one.mergeWith(two);
  // // Expected exception.
  // }
  //
  // private void verifyListElements(NebuloList list, NebuloElement[] elements) {
  // ListIterator iter = list.iterator();
  // for (NebuloElement elem : elements) {
  // assertTrue(iter.hasNext());
  // assertEquals(elem, iter.next());
  // }
  // assertFalse(iter.hasNext());
  // }
}
