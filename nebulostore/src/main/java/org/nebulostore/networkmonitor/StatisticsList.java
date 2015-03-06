package org.nebulostore.networkmonitor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * @author Piotr Malicki
 */
public class StatisticsList implements Serializable {

  private static final long serialVersionUID = -7171534369356732747L;

  private final Map<ConnectionAttribute, List<PeerConnectionSurvey>> statistics_ = new HashMap<>();

  /**
   * Weight of the current mean used when removing a single element.
   */
  private final double weightSingle_;

  /**
   * Weight of the current mean used when calculating overall mean value.
   */
  private final double weightMulti_;

  /**
   * Mean value of statistics that were removed from the list.
   */
  private final Map<ConnectionAttribute, Double> removedMeans_ = new HashMap<>();

  private final int maxAllowedSize_;

  public StatisticsList(double weightSingle, double weightMulti, int maxSize) {
    weightSingle_ = weightSingle;
    weightMulti_ = weightMulti;
    maxAllowedSize_ = maxSize;
  }

  /**
   * Adds new element to the list and removes the first one if the maximum size would be exceeded
   * after this addition. In such case the removedMean is updated using the following formula: <br>
   * removedMean = removedMean * weightSingle_ + removedElement * (1 - weightSingle_)
   *
   * @param element
   */
  public void add(PeerConnectionSurvey element) {
    if (!statistics_.containsKey(element.getAttribute())) {
      statistics_.put(element.getAttribute(), new LinkedList<PeerConnectionSurvey>());
    }
    if (statistics_.get(element.getAttribute()).size() == maxAllowedSize_) {
      PeerConnectionSurvey removedElem = statistics_.get(element.getAttribute()).remove(0);
      updateRemovedMean(removedElem);
    }
    statistics_.get(element.getAttribute()).add(element);
  }

  /**
   * Calculates weighted mean value of all statistics of given attribute that were added to the list
   * at any moment. The value is calculated by using the following formula:<br>
   * meanValue = removedMean * weightMulti_ + (mean of values from list) * (1 - weightMulti_)
   */
  public double calcWeightedMean(ConnectionAttribute attribute) {
    Double mean = removedMeans_.get(attribute);

    Double statsMean = null;
    if (statistics_.containsKey(attribute)) {
      double sum = 0.0;
      for (PeerConnectionSurvey pcs : statistics_.get(attribute)) {
        sum += pcs.getValue();
      }
      statsMean = sum / statistics_.get(attribute).size();
    }

    if (mean == null && statsMean == null) {
      mean = 0.0;
    } else if (mean == null) {
      mean = statsMean;
    } else if (statsMean != null) {
      mean = mean * weightMulti_ + statsMean * (1 - weightMulti_);
    }

    return mean;
  }

  public void addAllInFront(StatisticsList list) {
    for (ConnectionAttribute attribute : list.statistics_.keySet()) {
      if (!statistics_.containsKey(attribute)) {
        statistics_.put(attribute, Lists.newLinkedList(list.statistics_.get(attribute)));
        removedMeans_.put(attribute, list.removedMeans_.get(attribute));
      } else {
        List<PeerConnectionSurvey> statsList = statistics_.get(attribute);
        int freeSpacesNum = maxAllowedSize_ - statsList.size();
        ListIterator<PeerConnectionSurvey> iterator = list.statistics_.get(attribute).listIterator(
            list.statistics_.get(attribute).size());
        while (iterator.hasPrevious() && freeSpacesNum > 0) {
          statsList.add(0, iterator.previous());
          freeSpacesNum--;
        }

        if (removedMeans_.get(attribute) == null) {
          removedMeans_.put(attribute, list.removedMeans_.get(attribute));
        } else if (list.removedMeans_.get(attribute) != null) {
          // Calculate regular mean value of the mean values of lists being merged
          removedMeans_.put(attribute,
              (list.removedMeans_.get(attribute) + removedMeans_.get(attribute)) / 2.0);
        }

        List<PeerConnectionSurvey> statistics = list.statistics_.get(attribute);
        for (int i = 0; i <= iterator.previousIndex(); i++) {
          updateRemovedMean(statistics.get(i));
        }
      }
    }
  }

  public List<PeerConnectionSurvey> getAllStatisticsView() {
    return Lists.newLinkedList(Iterables.concat(statistics_.values()));
  }

  public int size() {
    int size = 0;
    for (List<PeerConnectionSurvey> list : statistics_.values()) {
      size += list.size();
    }
    return size;
  }

  public List<PeerConnectionSurvey> getStatistics(ConnectionAttribute attribute) {
    return statistics_.get(attribute);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + maxAllowedSize_;
    result = prime * result + ((removedMeans_ == null) ? 0 : removedMeans_.hashCode());
    result = prime * result + ((statistics_ == null) ? 0 : statistics_.hashCode());
    long temp;
    temp = Double.doubleToLongBits(weightMulti_);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(weightSingle_);
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
    StatisticsList other = (StatisticsList) obj;
    if (maxAllowedSize_ != other.maxAllowedSize_) {
      return false;
    }
    if (removedMeans_ == null) {
      if (other.removedMeans_ != null) {
        return false;
      }
    } else if (!removedMeans_.equals(other.removedMeans_)) {
      return false;
    }
    if (statistics_ == null) {
      if (other.statistics_ != null) {
        return false;
      }
    } else if (!statistics_.equals(other.statistics_)) {
      return false;
    }
    if (Double.doubleToLongBits(weightMulti_) != Double.doubleToLongBits(other.weightMulti_)) {
      return false;
    }
    if (Double.doubleToLongBits(weightSingle_) != Double.doubleToLongBits(other.weightSingle_)) {
      return false;
    }
    return true;
  }

  private void updateRemovedMean(PeerConnectionSurvey removedElem) {
    Double removedMean = removedMeans_.get(removedElem.getAttribute());
    if (removedMean == null) {
      removedMean = removedElem.getValue();
    } else {
      removedMean = removedMean * weightSingle_ + removedElem.getValue() * (1.0 - weightSingle_);
    }
    removedMeans_.put(removedElem.getAttribute(), removedMean);
  }

}
