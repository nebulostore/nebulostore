package org.nebulostore.appcore.model;

import java.util.List;

import org.nebulostore.crypto.EncryptWrapper;

/**
 * Interface for modules capable of writing NebuloObjects.
 * @author Bolek Kulbabinski
 */
public interface ObjectWriter extends GeneralObjectWriter {
  /**
   * Write the object asynchronously.
   * @param objectToWrite
   * @param previousVersionSHAs
   * @param encryptWrapper
   */
  void writeObject(NebuloObject objectToWrite, List<String> previousVersionSHAs,
      EncryptWrapper encryptWrapper);

}
