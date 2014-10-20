package org.nebulostore.appcore.model;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.subscription.model.SubscriptionNotification.NotificationReason;
import org.nebulostore.utils.Pair;

/**
 * List of NebuloObjects.
 */
/*
 * This is a new version of the NebuloListAPI.
 * Some of the below methods for now are exact same methods as in the old version of ListAPI;
 * others for now are just stub methods.
 */
public class NebuloList extends NebuloObject implements Iterable<NebuloElement> {
  private static Logger logger_ = Logger.getLogger(NebuloList.class);
  private static final long serialVersionUID = 3042380556591602347L;

  private List<NebuloElement> elements_;

  // Epoch and epoch's range are needed to aid synchronization process.
  private int epoch_;
  private Pair<Integer, Integer> epochRange_;

  // Attributes:
  boolean isPublicReadable_;
  boolean isPublicAppendable_;
  // The "delible" attribute specifies if "delete" operation can be performed on the list.
  boolean delible_;

  protected NebuloList(NebuloAddress address) {
    super(address);
    // TODO complete
    // constructs (creates/fetches) list with given address

    elements_ = new ArrayList<NebuloElement>();
    epoch_ = 1;
    epochRange_ = new Pair<Integer, Integer>(0, 0);
    isPublicReadable_ = true;
    isPublicAppendable_ = true;
    delible_ = true;
  }

  protected NebuloList(NebuloAddress address, long fromIndex, long toIndex) {
    super(address);
    // TODO implement
    // based on range constructs list which is a sublist of NebuloList with given address
  }

  protected NebuloList(NebuloAddress address, Comparator<NebuloElement> predicate) {
    super(address);
    // TODO implement
    // based on Predicate constructs list which is a sublist of NebuloList with given address
  }

  // Automatically synchronized.
  // TODO will need a new communicate to replica
  public void edit(List<NebuloElement> newSublist) {
    // TODO implement
    // substitutes this list on given (during construction) boundaries with a new list and
    // automatically synchronizes this change
  }

  // Automatically synchronized.
  // TODO will need a new communicate to replica
  public void append(List<NebuloElement> elementsToAppend) throws NebuloException {
    // TODO complete
    // appends a list of new elements to the end of the list and synchronizes this update as
    // specified

    elements_.addAll(elementsToAppend);
    sync();
  }

  // /**
  // * Designated to be used only by replicas. Appends elements to the local copy of the list.
  // *
  // * @param elements elements to be appended
  // */
  // public void localAppend(List<NebuloElement> elements) {
  // elements_.addAll(elements);
  // }

  // Automatically synchronized.
  public void delete(List<NebuloElement> elementsToDelete) {
    // TODO implement
    // verifies if the agent requesting "delete" is an author of the Element and then if
    // authenticated and the list is delible, put tombstones on requested for deletion entries;
    // synchronized automatically
  }

  /**
   * Updates local copy with current network version (as opposed to sync() fetches network copy
   * instead of pushing local copy).
   */
  public void update() {
    // TODO implement
    // updates local copy of the list with network copy
  }

  /**
   * Updates local sublist based on given range.
   */
  public void update(int fromIndex, int toIndex) {
    // TODO implement
  }

  /**
   * Updates local sublist based on given predicate.
   */
  public void update(Comparator<NebuloElement> predicate) {
    // TODO implement
  }

  @Override
  /**
   * Returns local iterator.
   */
  public Iterator<NebuloElement> iterator() {
    return elements_.listIterator();
  }

  /**
   * Returns local iterator, starting at a specified position.
   */
  public ListIterator<NebuloElement> iterator(int index) {
    return elements_.listIterator(index);
  }

  /**
   * Returns the element at the specified position in this list.
   * 
   * @param index
   *          index of the element to return
   * @return the element at the specified position in this list
   * @throws IndexOutOfBoundsException
   *           - if the index is out of range (index < 0 || index >= size())
   */
  public NebuloElement get(int index) throws IndexOutOfBoundsException {
    return elements_.get(index);
  }

  /**
   * Returns the number of elements in this list. If this list contains more than Integer.MAX_VALUE
   * elements, returns Integer.MAX_VALUE.
   * 
   * @return the number of elements in this list
   */
  public int getLocalSize() {
    return elements_.size();
  }

  /**
   * 
   * @return the network size of the list
   */
  public int getNetworkSize() {
    // TODO implement
    return 0;
  }

  // Methods inherited from NebuloObject.

  @Override
  /**
   * Attempts to conduct transaction in NebuloFile style (sends local version to replicators).
   */
  protected void runSync() throws NebuloException {
    logger_.info("Running sync() on list.");
    ObjectWriter writer = objectWriterProvider_.get();
    writer.writeObject(this, previousVersions_);

    try {
      writer.getSemiResult(TIMEOUT_SEC);
      writer.setAnswer(TransactionAnswer.COMMIT);
      writer.awaitResult(TIMEOUT_SEC);
      notifySubscribers(NotificationReason.FILE_CHANGED);
    } catch (NebuloException exception) {
      writer.setAnswer(TransactionAnswer.ABORT);
      throw exception;
    }
  }

  @Override
  /**
   * Attempts to delete list in NebuloObject style.
   */
  public void delete() throws NebuloException {
    logger_.info("Running delete() on list.");
    ObjectDeleter deleter = objectDeleterProvider_.get();
    deleter.deleteObject(address_);
    deleter.awaitResult(TIMEOUT_SEC);
    notifySubscribers(NotificationReason.FILE_DELETED);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = prime + ((elements_ == null) ? 0 : elements_.hashCode());
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
    NebuloList other = (NebuloList) obj;
    if (elements_ == null) {
      if (other.elements_ != null) {
        return false;
      }
    } else if (!elements_.equals(other.elements_)) {
      return false;
    }
    return true;
  }
}