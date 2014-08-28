package org.nebulostore.benchmarks.labtest;

import com.google.common.base.Function;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.addressing.NebuloAddress;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.model.NebuloFile;
import org.nebulostore.appcore.model.NebuloObjectFactory;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.benchmarks.labtest.messages.PerformRandomActionMessage;
import org.nebulostore.conductor.messages.ErrorMessage;
import org.nebulostore.crypto.CryptoUtils;
import org.nebulostore.dispatcher.JobInitMessage;
import org.nebulostore.persistence.SQLKeyValueStore;
import org.nebulostore.timer.Timer;

/**
 * Module that consecutively performs random activity (read or write) every 
 * specified period of time.
 * 
 * Results of the performed actions are stored in the database.
 * 
 * Properties of the DB are read from configuration.
 * 
 * @author Szymon Matejczyk
 *
 */
public class RandomActivityModule extends JobModule {
  private static Logger logger_ = Logger.getLogger(RandomActivityModule.class);
  
  private static final String CONFIGURATION_PREFIX = "random-activity-module.";
  
  /**
   * Delay before performing first action.
   */
  private static final Long INITIAL_DELAY = 1000L;
  
  /**
   * Nebulo objects encoding.
   */
  private static final String ENCODING = "UTF-8";
  
  /**
   * Database data.
   */
  private String databaseHostName_;
  private String databasePort_;
  private String databaseName_;
  private String databaseUserName_;
  private String databasePassword_;
  
  private Timer timer_;
  
  /**
   * Delay between performing consecutive actions.
   */
  private Long actionPeriodMillis_;
  
  /**
   * Running time.
   */
  private Integer runningTimeMinutes_;
  
  /**
   * Objects stored by the peer.
   */
  private Set<NebuloAddress> storedObjects_ = new TreeSet<>();
  
  /**
   * Configured peer name (read from configuration).
   */
  private String peerName_;
  
  private NebuloObjectFactory objectFactory_;
  
  @Inject
  public void setDependencies(
      @Named(CONFIGURATION_PREFIX + "database-hostname") String databaseHostName, 
      @Named(CONFIGURATION_PREFIX + "database-port") String databasePort,
      @Named(CONFIGURATION_PREFIX + "database-name") String databaseName,
      @Named(CONFIGURATION_PREFIX + "database-username") String databaseUserName,
      @Named(CONFIGURATION_PREFIX + "database-password") String databasePassword,
      Timer timer,
      @Named(CONFIGURATION_PREFIX + "action-period-millis") Long actionPeriodMillis,
      @Named(CONFIGURATION_PREFIX + "running-time-mins") Integer runningTime,
      NebuloObjectFactory nebuloObjectFactory) {
    databaseHostName_ = databaseHostName;
    databasePort_ = databasePort;
    databaseName_ = databaseName + 
        FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis( ));
    databaseUserName_ = databaseUserName;
    databasePassword_ = databasePassword;
    
    timer_ = timer;
    
    actionPeriodMillis_ = actionPeriodMillis;
    runningTimeMinutes_ = runningTime;
    
    objectFactory_ = nebuloObjectFactory;
  }
  
  private SQLKeyValueStore<String> database_;
  
  private class RandomActivityModuleVisitor extends MessageVisitor<Void> {
    
    public Void visit(JobInitMessage message) {
      logger_.debug("Received JobInitMessage");
      
      try {
        logger_.debug("Initializing database.");
        database_ = new SQLKeyValueStore<String>(databaseHostName_,
            databasePort_, databaseName_, databaseUserName_, databasePassword_, false,
            new Function<String, byte[]>() {
              @Override
              public byte[] apply(String arg0) {
                return arg0.getBytes();
              }
            }, new Function<byte[], String>() {
              @Override
              public String apply(byte[] arg0) {
                return new String(arg0);
              }
            });
        logToDatabase("STARTED");
        timer_.scheduleRepeated(new PerformRandomActionMessage(jobId_), INITIAL_DELAY, 
            actionPeriodMillis_);
        timer_.schedule(new EndModuleMessage(jobId_), runningTimeMinutes_ * 60 * 1000);
      } catch (IOException e) {
        logger_.warn("Unable to create SQL database.");
        outQueue_.add(new ErrorMessage(jobId_, null, null, "Database initialization failed"));
        endJobModule();
      }
      return null;
    }
    
    public Void visit(PerformRandomActionMessage message) {
      int action = CryptoUtils.nextInt(2);
      try {
        switch (action) {
        case 0: { // save
          BigInteger fileId = CryptoUtils.getRandomId();
          NebuloFile file = objectFactory_.createNewNebuloFile(
              new ObjectId(new BigInteger(fileId + "0")));
          try {
            file.write((peerName_ + fileId.toString()).getBytes(ENCODING), 0);
            logToDatabase("SAVED: " + fileId.toString());
            storedObjects_.add(file.getAddress());
          } catch (NebuloException e) {
            logger_.warn("Unable to write file.", e);
            logToDatabase("SAVE FAILED: " + fileId.toString());
          }
          break;
        }
        case 1: { // retrieve
          NebuloAddress fileAddress = randomStoredObject();
          if (fileAddress != null) {
            NebuloFile file;
            try {
              file = (NebuloFile) objectFactory_.fetchExistingNebuloObject(fileAddress);
              file.read(0, file.getSize());
              logToDatabase("RETRIEVE SUCCESSFUL");
            } catch (NebuloException e) {
              logger_.warn("Unable to retrieve file.", e);
              logToDatabase("RETRIEVE FAILED");
            }
          }
          break;
        }
        }
      } catch (IOException e) {
        logger_.warn("Unable to log action to database.", e);
      }
      return null;
    }
    
    public Void visit(EndModuleMessage message) {
      timer_.cancelTimer();
      endJobModule();
      return null;
    }
  }
  
  /**
   * @return Random element from {@code storedObjects_} or null when
   * none exists in the set.
   */
  private NebuloAddress randomStoredObject() {
    int pos = CryptoUtils.nextInt(storedObjects_.size());
    int i = 0;
    for (NebuloAddress address : storedObjects_) {
      if (i == pos)
        return address;
      else
        i++;
    }
    return null;
  }
  
  private MessageVisitor<Void> visitor_ = new RandomActivityModuleVisitor();

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }

  protected void logToDatabase(String message) {
    try {
      database_.put(generateTraceKey(), message);
    } catch (IOException e) {
      logger_.error("Unable to write to database. Finishing test.");
      timer_.cancelTimer();
      endJobModule();
    }
  }

  protected String generateTraceKey() {
    String dateString = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis( ));
    return  dateString + "_" + peerName_; 
  }
  
}
