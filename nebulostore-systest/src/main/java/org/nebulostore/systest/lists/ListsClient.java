package org.nebulostore.systest.lists;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloElement;
import org.nebulostore.appcore.model.NebuloList;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.ConductorClient;
import org.nebulostore.conductor.messages.NewPhaseMessage;
import org.nebulostore.crypto.CryptoException;
import org.nebulostore.crypto.CryptoUtils;

/**
 * Lists client.
 *
 * @author Bolek Kulbabinski
 */
public final class ListsClient extends ConductorClient {
  private static final long serialVersionUID = -7238750658102427676L;
  private static Logger logger_ = Logger.getLogger(ListsClient.class);
  private static final int MAX_ITER = 2;
  private static final int INITIAL_SLEEP_MILLIS = 8000;
  private static final int ADDRESS_EXCHANGE_TIMEOUT_MILLIS = 60 * 1000;
  private static final int ITER_SLEEP = 2000;
  private static final int N_CASES = 3;
  private static final int THREAD_POOL_SIZE = 5;

  private final List<CommAddress> clients_;
  private final List<NebuloAddress> addresses_;
  private final int clientId_;
  private final ListsStats stats_;
  private NebuloList myList_;
  private transient NebuloObjectFactory objectFactory_;

  public ListsClient(String serverJobId, CommAddress serverAddress, int numPhases,
      List<CommAddress> clients, int clientId) {
    super(serverJobId, numPhases, serverAddress);
    clients_ = new ArrayList<CommAddress>(clients);
    clientId_ = clientId;
    stats_ = new ListsStats();
    addresses_ = new ArrayList<NebuloAddress>();
  }

  @Inject
  public void setNebuloObjectFactory(NebuloObjectFactory objectFactory) {
    objectFactory_ = objectFactory;
  }

  @Override
  protected void initVisitors() {
    visitors_ = new TestingModuleVisitor[numPhases_ + 2];
    sleep(INITIAL_SLEEP_MILLIS);
    visitors_[0] = new EmptyInitializationVisitor();
    try {
      myList_ = createList();
      visitors_[1] = new AddressExchangeVisitor(clients_, addresses_, clientId_,
          myList_.getAddress(), INITIAL_SLEEP_MILLIS, ADDRESS_EXCHANGE_TIMEOUT_MILLIS);
      visitors_[2] = new ReadFilesVisitor();
      visitors_[3] = new DeleteFileVisitor();
      visitors_[4] = new LastPhaseVisitor(stats_);
    } catch (CryptoException e) {
      endWithError("Exception while encrypting object: " + e.getMessage());
    } catch (NebuloException e) {
      endWithError("Exception in list append: " + e.getMessage());
    }
  }

  private NebuloList createList() throws NebuloException {
    NebuloList list = objectFactory_.createNewNebuloList(new ObjectId(new BigInteger(
        (clientId_ + 1) + "000")));
    for (int i = 0; i < N_CASES; ++i) {
      BigInteger value = list.getObjectId().getKey().add(BigInteger.valueOf(i));
      list.append(new NebuloElement(CryptoUtils.encryptObject(value)));
      list.sync();
    }
    return list;
  }

  /**
   * Phase 2 - read all the files and verify.
   */
  protected final class ReadFilesVisitor extends TestingModuleVisitor {

    @Override
    public Void visit(NewPhaseMessage message) {
      /**
       * Single file fetcher.
       * @author Bolek Kulbabinski
       */
      class FileFetchTask implements Callable<NebuloList> {
        NebuloAddress address_;
        public FileFetchTask(NebuloAddress address) {
          address_ = address;
        }
        @Override
        public NebuloList call() {
          for (int iter = 1; iter <= MAX_ITER; ++iter) {
            try {
              return (NebuloList) objectFactory_.fetchExistingNebuloObject(address_);
            } catch (NebuloException e) {
              logger_.debug("Unable to fetch list " + address_ + " in iteration " + iter);
              sleep(ITER_SLEEP);
            }
          }
          return null;
        }
      }

      ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
      List<Future<NebuloList>> futures = new ArrayList<Future<NebuloList>>();

      for (NebuloAddress address : addresses_) {
        stats_.incTriedFiles();
        futures.add(threadPool.submit(new FileFetchTask(address)));
      }

      for (int i = 0; i < addresses_.size(); i++) {
        NebuloAddress address = addresses_.get(i);
        try {
          NebuloList list = futures.get(i).get();
          if (list == null) {
            unableToFetchList(address, "Got null from pool.");
            continue;
          }
          Iterator<NebuloElement> iterator = list.iterator();
          for (int j = 0; j < N_CASES; ++j) {
            BigInteger elem = (BigInteger) CryptoUtils.decryptObject(iterator.next().getData());
            BigInteger good = list.getObjectId().getKey().add(BigInteger.valueOf(j));
            if (!good.equals(elem)) {
              unableToFetchList(address, "Content is incorrect (" + elem + " != " + good + ")");
              continue;
            }
          }
          logger_.debug("Received correct list from address " + list.getAddress());
        } catch (InterruptedException e) {
          unableToFetchList(address, "InterruptedException");
        } catch (ExecutionException e) {
          unableToFetchList(address, "ExecutionException");
        } catch (CryptoException e) {
          unableToFetchList(address, "CryptoException");
        }
      }

      threadPool.shutdown();
      phaseFinished();
      return null;
    }

    private void unableToFetchList(NebuloAddress address, String reason) {
      logger_.debug("Unable to fetch list " + address + " (" + reason + ").");
      stats_.addAddress(address);
    }
  }

  /**
   * Phase 3 - delete my file.
   */
  protected final class DeleteFileVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      try {
        myList_.delete();
      } catch (NebuloException e) {
        endWithError("Unable to delete file (" + e.getMessage() + ")");
        return null;
      }
      phaseFinished();
      return null;
    }
  }
}
