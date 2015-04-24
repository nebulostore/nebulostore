package org.nebulostore.appcore.model;

import java.util.List;

/**
 * Interface for modules capable of writing NebuloObjects.
 * @author Bolek Kulbabinski
 */
public interface ObjectWriter extends GeneralObjectWriter {
  /**
   * Write the object asynchronously.
   * @param objectToWrite
   * @param previousVersionSHAs
   */
  void writeObject(NebuloObject objectToWrite, List<String> previousVersionSHAs);

}
