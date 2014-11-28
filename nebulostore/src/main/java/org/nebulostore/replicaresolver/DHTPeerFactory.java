package org.nebulostore.replicaresolver;

import java.util.concurrent.BlockingQueue;

import com.google.inject.assistedinject.Assisted;

import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.modules.Module;

/**
 * Factory interface for AssistedInject.
 *
 * @author Piotr Malicki
 *
 */
public interface DHTPeerFactory {
  Module createDHTPeer(
      @Assisted("communication.dht.inQueue") BlockingQueue<Message> inQueue,
      @Assisted("communication.dht.outQueue") BlockingQueue<Message> outQueue,
      ReplicaResolver replicaResolver);
}
