package org.nebulostore.query.privacy.level;

import org.nebulostore.query.privacy.PrivacyLevel;

public class PublicOthers extends PrivacyLevel {

  private static PublicOthers instance_;

  public static PrivacyLevel getInstance() {
    if (instance_ == null) {
      instance_ = new PublicOthers();
    }
    return instance_;
  }

  private PublicOthers() {

  }

  @Override
  public PrivacyLevel generalize(PrivacyLevel l) {
    // TODO: Proper privacy levels support
    return this;
  }

}
