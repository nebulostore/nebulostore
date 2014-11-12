package org.nebulostore.async.util;

import java.util.Set;

import org.nebulostore.communication.naming.CommAddress;

/**
 * Class storing data about recipients set remembered in the asynchronous messages context.
 *
 * @author Piotr Malicki
 *
 */
public class RecipientsData {

  private final Set<CommAddress> recipients_;
  private final int recipientsSetVersion_;

  public RecipientsData(Set<CommAddress> recipients, int recipientsSetVersion) {
    recipients_ = recipients;
    recipientsSetVersion_ = recipientsSetVersion;
  }

  public Set<CommAddress> getRecipients() {
    return recipients_;
  }

  public int getRecipientsSetVersion() {
    return recipientsSetVersion_;
  }
}
