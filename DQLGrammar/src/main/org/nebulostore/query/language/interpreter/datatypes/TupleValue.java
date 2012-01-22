package org.nebulostore.query.language.interpreter.datatypes;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.nebulostore.query.language.interpreter.exceptions.TypeException;
import org.nebulostore.query.privacy.PrivacyLevel;
import org.nebulostore.query.privacy.level.PublicMy;

public class TupleValue extends DQLValue {

  private final List<IDQLValue> values_;
  private final List<DQLType> types_;
  private final int size_;

  public TupleValue(Collection<IDQLValue> values, Collection<DQLType> types,
      PrivacyLevel privacyLevel) throws TypeException {
    super(privacyLevel);
    values_ = new LinkedList<IDQLValue>(values);
    types_ = new LinkedList<DQLType>(types);
    size_ = values_.size();
    if (size_ != types_.size()) {
      throw new TypeException(
          "Unable to build type with not specified or over specified types");
    }
    checkTypes();
  }

  public TupleValue() {
    super(PublicMy.getInstance());
    values_ = new LinkedList<IDQLValue>();
    types_ = new LinkedList<DQLValue.DQLType>();
    size_ = 0;
  }

  private void checkTypes() throws TypeException {
    for (int i = 0; i < size_; i++) {
      if (values_.get(i).getType() != types_.get(i)) {
        throw new TypeException("Wrong value type at position: " + i);
      }
    }
  }

  public IDQLValue get(int index) {
    return values_.get(index);
  }

  public int getSize() {
    return size_;
  }

  @Override
  public Object toJava() {
    return JavaValuesGlue.fromDQL(values_);
  }

  @Override
  public DQLType getType() {
    return DQLType.DQLTuple;
  }

  @Override
  public String toString() {
    String ret = "TupleValue(";
    for (IDQLValue value : values_) {
      ret += value.toString() + ", ";
    }
    return ret + ")";
  }
}
