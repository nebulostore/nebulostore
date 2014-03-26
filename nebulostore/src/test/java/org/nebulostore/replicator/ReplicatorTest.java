package org.nebulostore.replicator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Charsets;
import org.junit.Assert;
import org.junit.Test;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.persistence.InMemoryStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;

/**
 * @author Bolek Kulbabinski
 */
public class ReplicatorTest {
  private static final ObjectId ID_1 = new ObjectId(new BigInteger("111"));
  private static final String CONTENT_1 = "sample file content 1";

  @Test
  public void shouldGetExistingObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator()
        .store(ID_1.toString(), CONTENT_1)
        .store(ID_1.toString() + ".meta", "metadata");
    // when
    replicator.sendMsg(new GetObjectMessage(null, ID_1, null));
    // then
    Message result = replicator.receiveMsg();
    Assert.assertTrue(result instanceof SendObjectMessage);
    Assert.assertArrayEquals(CONTENT_1.getBytes(Charsets.UTF_8),
        ((SendObjectMessage) result).getEncryptedEntity().getEncryptedData());
    // clean
    endReplicator(replicator);
  }

  @Test
  public void shouldSendErrorMessageWhenGettingNonexistentObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    replicator.sendMsg(new GetObjectMessage(null, ID_1, null));
    // then
    Message result = replicator.receiveMsg();
    Assert.assertTrue(result instanceof ReplicatorErrorMessage);
    // clean
    endReplicator(replicator);
  }



  static class ReplicatorWrapper {
    public KeyValueStore<byte[]> store_ = new InMemoryStore<>();
    public Replicator replicator_ = new ReplicatorImpl(store_);
    public BlockingQueue<Message> inQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> outQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> networkQueue_ = new LinkedBlockingQueue<Message>();
    public Thread thread_ = new Thread(replicator_);

    {
      replicator_.setInQueue(inQueue_);
      replicator_.setOutQueue(outQueue_);
      replicator_.setNetworkQueue(networkQueue_);
    }

    public void sendMsg(Message msg) {
      inQueue_.add(msg);
    }

    public Message receiveMsg() throws InterruptedException {
      return networkQueue_.take();
    }

    public ReplicatorWrapper store(String key, String value) throws IOException {
      store_.put(key, value.getBytes(Charsets.UTF_8));
      return this;
    }
  }

  private ReplicatorWrapper startNewReplicator() {
    ReplicatorWrapper wrapper = new ReplicatorWrapper();
    wrapper.thread_.start();
    return wrapper;
  }

  private void endReplicator(ReplicatorWrapper wrapper) throws InterruptedException {
    wrapper.thread_.join();
  }
}
