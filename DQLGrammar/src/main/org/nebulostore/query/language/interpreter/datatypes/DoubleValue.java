package org.nebulostore.query.language.interpreter.datatypes;

import org.nebulostore.query.language.interpreter.exceptions.InterpreterException;
import org.nebulostore.query.language.interpreter.exceptions.TypeException;
import org.nebulostore.query.privacy.PrivacyLevel;

public class DoubleValue extends DQLValue {

  private final double value_;

  public DoubleValue(double value, PrivacyLevel privacyLevel) {
    super(privacyLevel);
    value_ = value;
  }

  public double getValue() {
    return value_;
  }

  @Override
  public IDQLValue addNum(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue) {
      return new DoubleValue(((IntegerValue) arg).getValue() + value_,
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else if (arg instanceof DoubleValue) {
      return new DoubleValue(((DoubleValue) arg).getValue() + value_,
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else
      throw new TypeException("Unable to add " + this.toString() + " to " +
          arg.toString());
  }

  @Override
  public IDQLValue subNum(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue) {
      return new DoubleValue(value_ - ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else if (arg instanceof DoubleValue) {
      return new DoubleValue(value_ - ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else
      throw new TypeException("Unable to substract " + this.toString() +
          " to " + arg.toString());
  }

  @Override
  public IDQLValue multNum(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue) {
      return new DoubleValue(((IntegerValue) arg).getValue() * value_,
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else if (arg instanceof DoubleValue) {
      return new DoubleValue(((DoubleValue) arg).getValue() * value_,
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else
      throw new TypeException("Unable to multiply " + this.toString() + " to " +
          arg.toString());
  }

  @Override
  public IDQLValue divNum(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue) {
      return new DoubleValue(value_ / ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else if (arg instanceof DoubleValue) {
      return new DoubleValue(value_ / ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    } else
      throw new TypeException("Unable to substract " + this.toString() +
          " to " + arg.toString());
  }

  @Override
  public IDQLValue numNegation() throws InterpreterException {
    return new DoubleValue(-value_, privacyLevel_);
  }

  @Override
  public IDQLValue equals(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue)
      return new BooleanValue(value_ == ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else if (arg instanceof DoubleValue)
      return new BooleanValue(value_ == ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else
      throw new TypeException("Unable to determine equality between " +
          this.toString() + " and " + arg.toString());
  }

  @Override
  public IDQLValue less(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue)
      return new BooleanValue(value_ < ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else if (arg instanceof DoubleValue)
      return new BooleanValue(value_ < ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else
      throw new TypeException("Unable to determine less between " +
          this.toString() + " and " + arg.toString());
  }

  @Override
  public IDQLValue lessEquals(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue)
      return new BooleanValue(value_ <= ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else if (arg instanceof DoubleValue)
      return new BooleanValue(value_ <= ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else
      throw new TypeException("Unable to determine less equals between " +
          this.toString() + " and " + arg.toString());
  }

  @Override
  public IDQLValue greater(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue)
      return new BooleanValue(value_ > ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else if (arg instanceof DoubleValue)
      return new BooleanValue(value_ > ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else
      throw new TypeException("Unable to determine greater between " +
          this.toString() + " and " + arg.toString());
  }

  @Override
  public IDQLValue greaterEquals(IDQLValue arg) throws InterpreterException {
    if (arg instanceof IntegerValue)
      return new BooleanValue(value_ >= ((IntegerValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else if (arg instanceof DoubleValue)
      return new BooleanValue(value_ >= ((DoubleValue) arg).getValue(),
          privacyLevel_.generalize(arg.getPrivacyLevel()));
    else
      throw new TypeException("Unable to determine greater equals between " +
          this.toString() + " and " + arg.toString());
  }

  @Override
  public IDQLValue notEquals(IDQLValue arg) throws InterpreterException {
    return new BooleanValue(!((BooleanValue) equals(arg)).getValue(),
        privacyLevel_.generalize(arg.getPrivacyLevel()));
  }

  @Override
  public String toString() {
    return "Double(" + value_ + ")";
  }

  @Override
  public Object toJava() {
    return value_;
  }

  @Override
  public DQLType getType() {
    return DQLType.DQLDouble;
  }

}