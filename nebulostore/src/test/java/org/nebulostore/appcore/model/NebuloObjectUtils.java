package org.nebulostore.appcore.model;

import java.math.BigInteger;

import com.google.inject.Provider;

import org.mockito.Mockito;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;


/**
 * @author Bolek Kulbabinski
 */
public final class NebuloObjectUtils {
  private NebuloObjectUtils() { }

  public static void setMockProviders(NebuloObject nebuloObject) {
    Provider<ObjectGetter> getterProvider = new Provider<ObjectGetter>() {
      @Override
      public ObjectGetter get() {
        return Mockito.mock(ObjectGetter.class);
      }
    };
    Provider<ObjectWriter> writerProvider = new Provider<ObjectWriter>() {
      @Override
      public ObjectWriter get() {
        return Mockito.mock(ObjectWriter.class);
      }
    };
    Provider<ObjectDeleter> deleterProvider = new Provider<ObjectDeleter>() {
      @Override
      public ObjectDeleter get() {
        return Mockito.mock(ObjectDeleter.class);
      }
    };
    nebuloObject.setProviders(getterProvider, writerProvider, deleterProvider);
  }

  public static NebuloAddress getNewNebuloAddress(String appKey, String objectId) {
    return new NebuloAddress(new AppKey(new BigInteger(appKey)),
        new ObjectId(new BigInteger(objectId)));
  }

  public static NebuloFile getNewNebuloFile(String appKey, String objectId) {
    NebuloFile file = new NebuloFile(NebuloObjectUtils.getNewNebuloAddress(appKey, objectId));
    setMockProviders(file);
    return file;
  }

  public static NebuloList getNewNebuloList(String appKey, String objectId) {
    NebuloList list = new NebuloList(NebuloObjectUtils.getNewNebuloAddress(appKey, objectId));
    setMockProviders(list);
    return list;
  }

  public static NebuloElement getNewNebuloElement(String appKey, String objectId) {
    NebuloElement element =
        new NebuloElement(NebuloObjectUtils.getNewNebuloAddress(appKey, objectId));
    return element;
  }
}
