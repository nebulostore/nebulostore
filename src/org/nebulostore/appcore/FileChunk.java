package org.nebulostore.appcore;

/**
 * Represents file chunk.
 */
public class FileChunk extends NebuloObject {

  private static final long serialVersionUID = -8992804393601229112L;

  protected byte[] data_;

  public FileChunk() {
    data_ = new byte[0];
  }

  public byte[] getData() {
    return data_;
  }

  public void setData(byte[] data) {
    data_ = data;
  }

  @Override
  protected void runSync() {
    // TODO Auto-generated method stub
  }
}
