package org.nebulostore.appcore.model;

import java.util.Set;

import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;

/**
 * @author rafalhryciuk
 * @author Bolek Kulbabinski
 */
public interface NebuloObjectFactory {

  NebuloFile createNewNebuloFile();

  NebuloFile createNewNebuloFile(ObjectId objectId);

  NebuloFile createNewNebuloFile(NebuloAddress address);

  NebuloFile createNewAccessNebuloFile(NebuloAddress address, Set<AppKey> accessList)
      throws NebuloException;

  NebuloList createNewNebuloList();

  NebuloList createNewNebuloList(ObjectId objectId);

  NebuloList createNewNebuloList(NebuloAddress address);

  NebuloObject fetchExistingNebuloObject(NebuloAddress address) throws NebuloException;

  NebuloObject fetchNebuloObject(NebuloAddress address) throws NebuloException;

  void deleteNebuloObject(NebuloAddress address) throws NebuloException;

}
