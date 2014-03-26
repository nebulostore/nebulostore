package org.nebulostore.dispatcher;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.inject.Injector;
import org.apache.log4j.Logger;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.EndModuleMessage;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.appcore.modules.Module;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dispatcher - core module that assigns threads to tasks and distributes messages
 *     among existing job modules.
 */
public class Dispatcher extends Module {
  private static Logger logger_ = Logger.getLogger(Dispatcher.class);
  private static final int JOIN_TIMEOUT_MILLIS = 3000;

  private final Map<String, BlockingQueue<Message>> workersQueues_;
  private final Map<String, Thread> workersThreads_;
  private final MessageVisitor<?> visitor_;
  private final Injector injector_;

  /**
   * Visitor class. Contains logic for handling messages depending
   * on their types.
   */
  public class MessageDispatchVisitor extends MessageVisitor<Void> {
    private static final int MAX_LOGGED_JOB_ID_LENGTH = 8;

    /*
     * Special handling for JobEndedMessage.
     * Remove MSG_ID from Dispatcher's map.
     */
    public Void visit(JobEndedMessage message) {
      if (message.getId() != null) {
        String jobId = message.getId();
        logger_.debug("Got job ended message with ID: " + jobId);
        if (workersQueues_.containsKey(jobId)) {
          workersQueues_.remove(jobId);
        }
        if (workersThreads_.containsKey(jobId)) {
          try {
            workersThreads_.get(jobId).join(JOIN_TIMEOUT_MILLIS);
          } catch (InterruptedException e) {
            logger_.warn("InterruptedException while waiting for a dying thread to join. JobID = " +
                jobId);
          }
          workersThreads_.remove(jobId);
        }
      } else {
        logger_.debug("Got job ended message with NULL ID.");
      }
      return null;
    }

    /*
     * End dispatcher.
     */
    public Void visit(EndModuleMessage message) throws NebuloException {
      Thread[] threads =
          workersThreads_.values().toArray(new Thread[workersThreads_.values().size()]);
      logger_.debug("Quitting dispatcher, waiting for " + threads.length + " job threads.");
      for (int i = 0; i < threads.length; ++i) {
        try {
          threads[i].join(JOIN_TIMEOUT_MILLIS);
        } catch (InterruptedException exception) {
          continue;
        }
      }
      endModule();
      return null;
    }

    /*
     * General behavior - forwarding messages.
     */
    public Void visitDefault(Message message) throws NebuloException {
      if (message.getId() != null) {
        String jobId = message.getId();
        logger_.debug("Received message with jobID: " + jobId + " and class name: " +
            message.getClass().getSimpleName());

        // TODO(bolek): Maybe move injection to deserialization process?
        injector_.injectMembers(message);
        if (!workersQueues_.containsKey(jobId)) {
          // Spawn a new thread to handle the message.
          try {
            JobModule handler = message.getHandler();
            BlockingQueue<Message> newInQueue = new LinkedBlockingQueue<Message>();
            handler.setInQueue(newInQueue);
            handler.setJobId(jobId);
            // TODO(bolek): Remove this. Message injection should be sufficient.
            injector_.injectMembers(handler);
            if (handler.isQuickNonBlockingTask()) {
              logger_.debug("Executing in current thread with handler of type " +
                  handler.getClass().getSimpleName());
              handler.run();
            } else {
              Thread newThread = new Thread(handler, handler.getClass().getSimpleName() + ":" +
                  jobId.substring(0, Math.min(MAX_LOGGED_JOB_ID_LENGTH, jobId.length())));
              newThread.setDaemon(true);
              workersQueues_.put(jobId, newInQueue);
              workersThreads_.put(jobId, newThread);
              logger_.debug("Starting new thread with handler of type " +
                  handler.getClass().getSimpleName());
              newThread.start();
              newInQueue.add(message);
            }
          } catch (NebuloException exception) {
            logger_.debug("Message does not contain a handler.");
          }
        } else {
          logger_.debug("Delegating message to an existing worker thread.");
          workersQueues_.get(jobId).add(message);
        }
        return null;
      } else {
        logger_.debug("Received message with NULL jobID and class name: " +
            message.getClass().getSimpleName());
      }
      return null;
    }
  }

  /**
   *
   * @param inQueue Input message queue.
   * @param outQueue Output message queue, which is also later passed to newly
   *                 created tasks (usually network's inQueue).
   */
  public Dispatcher(BlockingQueue<Message> inQueue,
      BlockingQueue<Message> outQueue, Injector injector) {
    super(inQueue, outQueue);
    visitor_ = new MessageDispatchVisitor();
    workersQueues_ = new TreeMap<String, BlockingQueue<Message>>();
    workersThreads_ = new TreeMap<String, Thread>();
    injector_ = checkNotNull(injector);
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    // Handling logic lies inside our visitor class.
    message.accept(visitor_);
  }
}
