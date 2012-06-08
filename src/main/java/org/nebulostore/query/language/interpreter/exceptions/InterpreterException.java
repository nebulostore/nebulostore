package org.nebulostore.query.language.interpreter.exceptions;

public class InterpreterException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public InterpreterException(String string) {
    super(string);
  }

  public InterpreterException(Throwable t) {
    super(t);
  }

  public InterpreterException(String message, Throwable cause) {
    super(message, cause);
  }
}