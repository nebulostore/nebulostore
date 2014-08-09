package org.nebulostore.utils;

import java.io.Serializable;

/**
 * @author szymonmatejczyk
 * @param <A>
 *          first element
 * @param <B>
 *          second element
 */
public class Pair<A, B> implements Serializable {
  private static final long serialVersionUID = 5172737595679197273L;

  private final A first_;
  private final B second_;

  public Pair(A a, B b) {
    first_ = a;
    second_ = b;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Pair)) {
      return false;
    }
    Pair other = (Pair) obj;
    if (first_ == null) {
      if (other.first_ != null) {
        return false;
      }
    } else if (!first_.equals(other.first_)) {
      return false;
    }
    if (second_ == null) {
      if (other.second_ != null) {
        return false;
      }
    } else if (!second_.equals(other.second_)) {
      return false;
    }
    return true;
  }

  public A getFirst() {
    return first_;
  }

  public B getSecond() {
    return second_;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((first_ == null) ? 0 : first_.hashCode());
    result = prime * result + ((second_ == null) ? 0 : second_.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "Pair<" + first_.getClass() + ", " + second_.getClass() + ">: (" + first_ +
        ", " + second_ + ")";
  }
}
