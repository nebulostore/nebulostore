package org.nebulostore.systest.readwrite;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.conductor.messages.NewPhaseMessage;

/**
 * @author hryciukrafal
 *
 *         Every peer writes data from local file. Next every peer reads all files.
 *         Test measures time of writes and reads.
 *         Test file should be put in a main project directory and its name should be passed as a
 *         test param.
 */
public final class ReadWriteTimeClient extends ReadWriteClient {

  private static final Logger LOGGER = Logger.getLogger(ReadWriteTimeClient.class);

  private static final long serialVersionUID = -7238750658102427676L;

  private static final String READ_WRITE_TIMES_FILE = "logs/readWriteTimes";

  private final String testDataFile_;


  public ReadWriteTimeClient(String serverJobId, CommAddress serverAddress, int numPhases,
                             List<CommAddress> clients, int clientId, String testDataFile) {
    super(serverJobId, serverAddress, numPhases, clients, clientId);
    this.testDataFile_ = testDataFile;
  }

  @Override
  protected void initVisitors() {
    visitors_ = new TestingModuleVisitor[numPhases_ + 2];
    visitors_[0] = new EmptyInitializationVisitor();
    myFile_ = createFile();
    visitors_[1] = buildAddressExchangeVisitor();
    visitors_[2] = new ReadFilesVisitor();
    visitors_[3] = new DeleteFileVisitor();
    visitors_[4] = buildLastPhaseVisitor();
  }

  private NebuloFile createFile() {
    NebuloFile file = objectFactory_.createNewNebuloFile(
        new ObjectId(new BigInteger((clientId_ + 1) + "000")));
    try {
      byte[] data = readFile(testDataFile_);
      final long startWriting = System.nanoTime();
      file.write(data, 0);
      final long endWriting = System.nanoTime();
      final long writeTime = endWriting - startWriting;
      writeToFile(ImmutableList.of("WRITE, " + file.getAddress() + ", " + writeTime));
      return file;
    } catch (NebuloException exception) {
      endWithError("Unable to write NebuloFile (" + exception.getMessage() + ")");
      return null;
    }
  }

  private byte[] readFile(String fileName) {
    try {
      return Files.readAllBytes(Paths.get(fileName));
    } catch (IOException e) {
      throw new RuntimeException("Could not read file " + fileName);
    }
  }

  private void writeToFile(Collection<String> data) {
    try {
      Files.write(Paths.get(READ_WRITE_TIMES_FILE), data, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      LOGGER.error("could not write to result file", e);
    }
  }

  /**
   * Phase 2 - read all the files and verify.
   */
  protected final class ReadFilesVisitor extends TestingModuleVisitor {
    @Override
    public Void visit(NewPhaseMessage message) {
      List<String> times = new ArrayList<>(files_.size());
      for (NebuloAddress address : files_) {
        boolean fetched = false;
        // Try to fetch each file at most MAX_ITER times.
        byte[] dataToVerify = readFile(testDataFile_);
        final int dataSize = dataToVerify.length;
        for (int iter = 1; iter <= MAX_ITER; ++iter) {
          try {
            final long startReading = System.nanoTime();
            NebuloFile file = (NebuloFile) objectFactory_.fetchExistingNebuloObject(address);
            byte[] content = file.read(0, dataSize);
            final long endReading = System.nanoTime();
            if (!Arrays.equals(content, dataToVerify)) {
              endWithError("File content is incorrect (" + new String(content, "UTF-8") + ")");
              return null;
            } else {
              LOGGER.debug("Received correct file from address " + address);
              fetched = true;
              final long readTime = endReading - startReading;
              times.add("READ, " + file.getAddress() + ", " + readTime);
              break;
            }
          } catch (NebuloException e) {
            LOGGER.debug("Unable to fetch file with address " + address + " in iteration " + iter);
          } catch (UnsupportedEncodingException e) {
            LOGGER.debug("Unable to decode received string in UTF-8.");
          }
          sleep(ITER_SLEEP);
        }
        if (!fetched) {
          stats_.addAddress(address);
        }
      }
      writeToFile(times);
      phaseFinished();
      return null;
    }
  }
}
