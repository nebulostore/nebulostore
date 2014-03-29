package org.nebulostore.networkmonitor;

import java.io.Serializable;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Class that holds data of one peers connections survey.
 *
 * @author szymonmatejczyk
 *
 */
public class PeerConnectionSurvey implements Serializable {
  private static final long serialVersionUID = -5595489110765197257L;

  private final CommAddress investigator_;
  private final long time_;
  private final ConnectionAttribute attribute_;
  private final double value_;

  public PeerConnectionSurvey(CommAddress investigator, long time, ConnectionAttribute attribute,
      double value) {
    investigator_ = investigator;
    time_ = time;
    attribute_ = attribute;
    value_ = value;
  }

  public CommAddress getInvestigator() {
    return investigator_;
  }

  public long getTime() {
    return time_;
  }

  public ConnectionAttribute getAttribute() {
    return attribute_;
  }

  public double getValue() {
    return value_;
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((attribute_ == null) ? 0 : attribute_.hashCode());
    result = prime * result + ((investigator_ == null) ? 0 : investigator_.hashCode());
    result = prime * result + (int) (time_ ^ (time_ >>> 32));
    long temp;
    temp = Double.doubleToLongBits(value_);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PeerConnectionSurvey other = (PeerConnectionSurvey) obj;
    if (attribute_ != other.attribute_) {
      return false;
    }
    if (investigator_ == null) {
      if (other.investigator_ != null) {
        return false;
      }
    } else if (!investigator_.equals(other.investigator_)) {
      return false;
    }
    if (time_ != other.time_) {
      return false;
    }
    if (Double.doubleToLongBits(value_) != Double.doubleToLongBits(other.value_)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return String.format("PeerConnectionSurvey:%s=%f (investigator: %s)", attribute_.toString(),
        value_, investigator_.toString());
  }

}
