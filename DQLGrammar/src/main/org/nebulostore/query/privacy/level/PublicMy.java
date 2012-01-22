package org.nebulostore.query.privacy.level;

import org.nebulostore.query.privacy.PrivacyLevel;

public class PublicMy extends PrivacyLevel {

  private static PublicMy instance_;

  public static PrivacyLevel getInstance() {
    if (instance_ == null) {
      instance_ = new PublicMy();
    }
    return instance_;
  }

  private PublicMy() {

  }

  @Override
  public PrivacyLevel generalize(PrivacyLevel l) {
    // TODO: Proper privacy levels support
    return this;
  }

}
