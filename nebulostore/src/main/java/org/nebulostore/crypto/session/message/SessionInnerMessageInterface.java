package org.nebulostore.crypto.session.message;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author lukaszsiczek
 */
public interface SessionInnerMessageInterface {

  String getSourceJobId();

  CommAddress getSourceAddress();

  CommAddress getDestinationAddress();

}
