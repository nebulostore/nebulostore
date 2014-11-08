package org.nebulostore.appcore.model;

import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;

import com.google.common.base.Predicate;

/**
 * @author rafalhryciuk
 * @author Bolek Kulbabinski
 */
public interface NebuloObjectFactory {

  NebuloFile createNewNebuloFile();

  NebuloFile createNewNebuloFile(ObjectId objectId);

  NebuloFile createNewNebuloFile(NebuloAddress address);

  NebuloList createNewNebuloList();

  NebuloList createNewNebuloList(ObjectId objectId);

  NebuloList createNewNebuloList(NebuloAddress address);

  NebuloObject fetchExistingNebuloObject(NebuloAddress address) throws NebuloException;

  NebuloList fetchExistingNebuloList(NebuloAddress address) throws NebuloException;

  NebuloList fetchExistingNebuloList(
      NebuloAddress address, int fromIndex, int toIndex)throws NebuloException;

  NebuloList fetchExistingNebuloList(NebuloAddress address, Predicate<NebuloElement> predicate)
      throws NebuloException;

  NebuloList fetchExistingNebuloList(NebuloAddress address, int fromIndex, int toIndex,
      Predicate<NebuloElement> predicate) throws NebuloException;
}
