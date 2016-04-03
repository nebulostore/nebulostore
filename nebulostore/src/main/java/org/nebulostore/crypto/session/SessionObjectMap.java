package org.nebulostore.crypto.session;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public class SessionObjectMap implements Map<CommAddress, SessionObject> {

  Map<CommAddress, SessionObject> map_ = new HashMap<CommAddress, SessionObject>();

  @Override
  public void clear() {
    map_.clear();
  }

  @Override
  public boolean containsKey(Object arg0) {
    return map_.containsKey(arg0);
  }

  @Override
  public boolean containsValue(Object arg0) {
    return map_.containsValue(arg0);
  }

  @Override
  public Set<Entry<CommAddress, SessionObject>> entrySet() {
    return map_.entrySet();
  }

  @Override
  public SessionObject get(Object arg0) {
    return map_.get(arg0);
  }

  @Override
  public boolean isEmpty() {
    return map_.isEmpty();
  }

  @Override
  public Set<CommAddress> keySet() {
    return map_.keySet();
  }

  @Override
  public SessionObject put(CommAddress arg0, SessionObject arg1) {
    return map_.put(arg0, arg1);
  }

  @Override
  public void putAll(Map<? extends CommAddress, ? extends SessionObject> arg0) {
    map_.putAll(arg0);
  }

  @Override
  public SessionObject remove(Object arg0) {
    return map_.remove(arg0);
  }

  @Override
  public int size() {
    return map_.size();
  }

  @Override
  public Collection<SessionObject> values() {
    return map_.values();
  }

}
