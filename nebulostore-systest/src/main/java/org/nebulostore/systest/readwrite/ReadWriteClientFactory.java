package org.nebulostore.systest.readwrite;

import java.util.List;

import org.nebulostore.communication.naming.CommAddress;

/**
 * @author hryciukrafal
 */
public interface ReadWriteClientFactory {
  ReadWriteClient createReadWriteClient(String serverJobId, CommAddress serverAddress,
                                        int numPhases, List<CommAddress> clients, int clientId);
}
