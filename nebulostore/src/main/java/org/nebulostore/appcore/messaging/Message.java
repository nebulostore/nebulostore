package org.nebulostore.appcore.messaging;

import java.io.Serializable;

import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Base class for messages.
 */
public abstract class Message implements Serializable, Comparable<Message> {
  private static final long serialVersionUID = -2032656006415029507L;

  // A unique id for message type
  protected final String id_;

  // ID used by Dispatcher to forward the message to proper thread (= running JobModule).
  protected final String jobId_;

  public Message() {
    jobId_ = CryptoUtils.getRandomString();
    id_ = CryptoUtils.getRandomString();
  }

  public Message(String jobID) {
    jobId_ = jobID;
    id_ = CryptoUtils.getRandomString();
  }

  public String getId() {
    return jobId_;
  }

  public String getMessageId() {
    return id_;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Message message = (Message) o;

    if (id_ != null ? !id_.equals(message.id_) : message.id_ != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id_ != null ? id_.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Message{" +
        "jobId_='" + jobId_ + "', " +
        "id_ ='" + id_ + '\'' +
        "} " + super.toString();
  }

  @Override
  public int compareTo(Message message) {
    return id_.compareTo(message.id_);
  }

  /**
   * Accept method required by the visitor pattern.
   */
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  public JobModule getHandler() throws NebuloException {
    // TODO(bolek): Change it into a more specific exception type.
    throw new NebuloException(getClass().getSimpleName() + " is not an initializing message type.");
  }
}
