package org.nebulostore.async.synchronization;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Class representing a Value of the vector clock.
 *
 * @author Piotr Malicki
 *
 */
public class VectorClockValue implements Comparable<VectorClockValue>, Serializable {

  private static final long serialVersionUID = -5356727256100942699L;

  /**
   * Value of the vector clock for each peer from proper group.
   */
  private final Map<CommAddress, ClockValue> values_ = new HashMap<>();

  private final CommAddress myAddress_;

  public VectorClockValue(CommAddress myAddress, int counterValue) {
    myAddress_ = myAddress;
    values_.put(myAddress_, new ClockValue(0, counterValue));
  }

  public synchronized void tick() {
    values_.get(myAddress_).tick();
  }

  public synchronized void updateClockValue(VectorClockValue value) {
    for (Entry<CommAddress, ClockValue> entry : value.values_.entrySet()) {
      ClockValue currentClockVal = values_.get(entry.getKey());
      if (currentClockVal == null || currentClockVal.compareTo(entry.getValue()) < 0) {
        values_.put(entry.getKey(), entry.getValue().copy());
      }
    }

  }

  /**
   * Compare values lexicographically.
   */
  @Override
  public int compareTo(VectorClockValue value) {
    Set<CommAddress> addresses = Sets.newTreeSet(values_.keySet());
    addresses.addAll(value.values_.keySet());
    for (CommAddress address : addresses) {
      ClockValue firstVal = new ClockValue(0, 0);
      if (values_.containsKey(address)) {
        firstVal = values_.get(address);
      }

      ClockValue secondVal = new ClockValue(0, 0);
      if (value.values_.containsKey(address)) {
        secondVal = value.values_.get(address);
      }

      if (firstVal.compareTo(secondVal) < 0) {
        return -1;
      } else if (firstVal.compareTo(secondVal) > 0) {
        return 1;
      }
    }
    return 0;
  }

  public synchronized VectorClockValue getValueCopy() {
    VectorClockValue valueCopy = new VectorClockValue(myAddress_, 0);
    valueCopy.values_.putAll(values_);
    return valueCopy;
  }

  public synchronized boolean areAllElementsLowerEqual(VectorClockValue value) {
    return compareAllElements(value, new Operation() {
      @Override
      public boolean compare(ClockValue firstVal, ClockValue secondVal) {
        return firstVal.compareTo(secondVal) <= 0;
      }
    });
  }

  public synchronized boolean areAllElementsGreater(VectorClockValue value) {
    return compareAllElements(value, new Operation() {
      @Override
      public boolean compare(ClockValue firstVal, ClockValue secondVal) {
        return firstVal.compareTo(secondVal) > 0;
      }
    });
  }

  @Override
  public String toString() {
    return "VectorClockValue: " + values_;
  }

  private boolean compareAllElements(VectorClockValue value, Operation op) {
    Set<CommAddress> addresses = Sets.newTreeSet(values_.keySet());
    addresses.addAll(value.values_.keySet());
    for (CommAddress address : addresses) {
      ClockValue firstVal = new ClockValue(0, 0);
      if (values_.keySet().contains(address)) {
        firstVal = values_.get(address);
      }

      ClockValue secondVal = new ClockValue(0, 0);
      if (value.values_.keySet().contains(address)) {
        secondVal = value.values_.get(address);
      }

      if (!op.compare(firstVal, secondVal)) {
        return false;
      }
    }
    return true;
  }

  private interface Operation {
    boolean compare(ClockValue firstValue, ClockValue secondValue);
  }

  private class ClockValue implements Comparable<ClockValue>, Serializable {

    private static final long serialVersionUID = -3430725994817691084L;

    /**
     * Current value of the clock for given peer.
     */
    private int value_;

    /**
     * Current value of counter (number of times peer was added to this synchro peer set).
     */
    private final int counter_;

    public ClockValue(int value, int counter) {
      value_ = value;
      counter_ = counter;
    }

    public void tick() {
      value_++;
    }

    public ClockValue copy() {
      return new ClockValue(value_, counter_);
    }

    @Override
    public String toString() {
      return "(" + counter_ + ", " + value_ + ")";
    }

    @Override
    public int compareTo(ClockValue val) {
      int result = Integer.compare(counter_, val.counter_);
      if (result == 0) {
        return Integer.compare(value_, val.value_);
      }

      return result;
    }
  }

}
