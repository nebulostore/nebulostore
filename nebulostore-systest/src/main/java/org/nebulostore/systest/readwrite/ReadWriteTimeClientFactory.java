package org.nebulostore.systest.readwrite;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.nebulostore.communication.naming.CommAddress;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author hryciukrafal
 */
public class ReadWriteTimeClientFactory implements ReadWriteClientFactory {

  private final String testDataFile_;


  @Inject
  public ReadWriteTimeClientFactory(@Named("data-file")String testDataFile) {
    this.testDataFile_ = checkNotNull(testDataFile);
  }

  @Override
  public ReadWriteClient createReadWriteClient(String serverJobId, CommAddress serverAddress,
                                               int numPhases, List<CommAddress> clients,
                                               int clientId) {
    return new ReadWriteTimeClient(serverJobId, serverAddress, numPhases, clients, clientId,
        testDataFile_);
  }

}
