package org.nebulostore.systest.readwrite;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.AppKey;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.NewPhaseMessage;

/**
 * ReadWrite client.
 *
 * @author Bolek Kulbabinski
 */
public class ReadWriteClient extends ConductorClient {
  private static final long serialVersionUID = -7238750658102427676L;
  private static Logger logger_ = Logger.getLogger(ReadWriteClient.class);
  protected static final int MAX_ITER = 10;
  private static final int INITIAL_SLEEP = 5000;
  private static final int ADDRESS_EXCHANGE_TIMEOUT_MILLIS = 60 * 1000;
  protected static final int ITER_SLEEP = 500;

  private List<CommAddress> clients_;
  protected List<NebuloAddress> files_;
  protected AppKey myAppKey_;
  protected int clientId_;
  protected NebuloFile myFile_;
  protected transient NebuloObjectFactory objectFactory_;
  protected final ReadWriteStats stats_;

  public ReadWriteClient(String serverJobId, CommAddress serverAddress, int numPhases,
      List<CommAddress> clients, int clientId) {
    super(serverJobId, numPhases, serverAddress);
    clients_ = new ArrayList<CommAddress>(clients);
    clientId_ = clientId;
    files_ = new ArrayList<NebuloAddress>();
    stats_ = new ReadWriteStats();
  }

  @Inject
  public void setAppKey(AppKey appKey) {
    myAppKey_ = appKey;
  }

  @Inject
  public void setNebuloObjectFactory(NebuloObjectFactory objectFactory) {
    objectFactory_ = objectFactory;
  }

  @Override
  protected void initVisitors() {
    visitors_ =  new TestingModuleVisitor[numPhases_ + 2];
    visitors_[0] = new EmptyInitializationVisitor();
    myFile_ = createFile();
    visitors_[1] = buildAddressExchangeVisitor();
    visitors_[2] = new ReadFilesVisitor();
    visitors_[3] = new DeleteFileVisitor();
    visitors_[4] = buildLastPhaseVisitor();
  }

  protected LastPhaseVisitor buildLastPhaseVisitor() {
    return new LastPhaseVisitor(stats_);
  }

  protected AddressExchangeVisitor buildAddressExchangeVisitor() {
    return new AddressExchangeVisitor(clients_, files_, clientId_, myFile_.getAddress(),
        INITIAL_SLEEP, ADDRESS_EXCHANGE_TIMEOUT_MILLIS);
  }

  private NebuloFile createFile() {
    NebuloFile file = objectFactory_.createNewNebuloFile(
        new ObjectId(new BigInteger((clientId_ + 1) + "000")));
    try {
      file.write(myAppKey_.getKey().toString().getBytes("UTF-8"), 0);
      return file;
    } catch (NebuloException exception) {
      endWithError("Unable to write NebuloFile (" + exception.getMessage() + ")");
      return null;
    } catch (UnsupportedEncodingException e) {
      endWithError("Unable to encode string in UTF-8.");
      return null;
    }
  }

  /**
   * Phase 2 - read all the files and verify.
   */
  protected final class ReadFilesVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      for (NebuloAddress address : files_) {
        boolean fetched = false;
        // Try to fetch each file at most MAX_ITER times.
        for (int iter = 1; iter <= MAX_ITER; ++iter) {
          try {
            NebuloFile file = (NebuloFile) objectFactory_.fetchExistingNebuloObject(address);
            byte[] content = file.read(0, 1000);
            if (!Arrays.equals(content,
                address.getAppKey().getKey().toString().getBytes("UTF-8"))) {
              endWithError("File content is incorrect (" + new String(content, "UTF-8") + ")");
              return null;
            } else {
              logger_.debug("Received correct file from address " + address);
              fetched = true;
              break;
            }
          } catch (NebuloException e) {
            logger_.debug("Unable to fetch file with address " + address + " in iteration " + iter);
          } catch (UnsupportedEncodingException e) {
            logger_.debug("Unable to decode received string in UTF-8.");
          }
          sleep(ITER_SLEEP);
        }
        if (!fetched) {
          stats_.addAddress(address);
        }
      }
      phaseFinished();
      return null;
    }
  }

  /**
   * Phase 3 - delete my file.
   */
  protected final class DeleteFileVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      try {
        myFile_.delete();
      } catch (NebuloException e) {
        endWithError("Unable to delete file (" + e.getMessage() + ")");
        return null;
      }
      phaseFinished();
      return null;
    }
  }
}
