package org.nebulostore.communication.naming;

import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Pattern;

import org.nebulostore.dht.core.KeyDHT;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Persistent identifier of NebuloStore instance.
 * (immutable)
 * @author Grzegorz Milka
 */
public final class CommAddress implements Serializable, Comparable<CommAddress> {
  private static final long serialVersionUID = -1730034659685291738L;
  private final UUID uuid_;
  private static CommAddress zeroCommAddress_;

  /**
   * Creates CommAddress's UUID from string.
   * String can be an integer or an UUID formatted string.
   */
  public CommAddress(String commAddress) {
    checkNotNull(commAddress);
    String uuidPattern = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-" +
        "[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}";
    if (Pattern.matches(uuidPattern, commAddress)) {
      uuid_ = UUID.fromString(commAddress);
    } else if (!commAddress.isEmpty()) {
      uuid_ = new UUID(0, Integer.parseInt(commAddress));
    } else {
      uuid_ = UUID.randomUUID();
    }
  }

  public CommAddress(long mostSigBits, long leastSigBits) {
    uuid_ = new UUID(mostSigBits, leastSigBits);
  }

  public CommAddress(UUID uuid) {
    uuid_ = uuid;
  }

  public static CommAddress newRandomCommAddress() {
    return new CommAddress(UUID.randomUUID());
  }

  /**
   * Return CommAddress symbolizing any or none address depending on context.
   */
  public static synchronized CommAddress getZero() {
    if (zeroCommAddress_ == null) {
      zeroCommAddress_ = new CommAddress(0L, 0L);
    }
    return zeroCommAddress_;
  }

  public UUID getPeerId() {
    return uuid_;
  }

  @Override
  public boolean equals(Object o) {
    return (o instanceof CommAddress) &&
      (uuid_.equals(((CommAddress) o).uuid_));
  }

  @Override
  public int hashCode() {
    return uuid_.hashCode();
  }

  @Override
  public String toString() {
    return uuid_.toString();
  }

  public KeyDHT toKeyDHT() {
    return KeyDHT.fromSerializableObject(this);
  }

  @Override
  public int compareTo(CommAddress other) {
    return uuid_.compareTo(other.uuid_);
  }
}
