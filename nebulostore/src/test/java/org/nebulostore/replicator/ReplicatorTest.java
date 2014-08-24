package org.nebulostore.replicator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;
import org.nebulostore.appcore.addressing.ObjectId;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.model.EncryptedObject;
import org.nebulostore.persistence.InMemoryStore;
import org.nebulostore.persistence.KeyValueStore;
import org.nebulostore.replicator.core.Replicator;
import org.nebulostore.replicator.core.TransactionAnswer;
import org.nebulostore.replicator.messages.ConfirmationMessage;
import org.nebulostore.replicator.messages.DeleteObjectMessage;
import org.nebulostore.replicator.messages.GetObjectMessage;
import org.nebulostore.replicator.messages.QueryToStoreObjectMessage;
import org.nebulostore.replicator.messages.ReplicatorErrorMessage;
import org.nebulostore.replicator.messages.SendObjectMessage;
import org.nebulostore.replicator.messages.TransactionResultMessage;

/**
 * @author Bolek Kulbabinski
 */
public class ReplicatorTest {
  private static final ObjectId ID_1 = new ObjectId(new BigInteger("111"));
  private static final ObjectId ID_2 = new ObjectId(new BigInteger("222"));
  private static final String CONTENT_1 = "sample file content 1";
  private static final String CONTENT_2 = "another file content 2";

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

  @Test
  public void shouldSaveObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    storeObject(replicator, ID_1, CONTENT_1);
    // then
    byte[] retrieved = replicator.store_.get(ID_1.toString());
    Assert.assertNotNull("Object not saved", retrieved);
    Assert.assertEquals(CONTENT_1, new String(retrieved, Charsets.UTF_8));
  }

  @Test
  public void shouldDeleteObject() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator()
        .store(ID_1.toString(), CONTENT_1);
    Assert.assertNotNull("Object not stored", replicator.store_.get(ID_1.toString()));
    // when
    deleteObject(replicator, ID_1);
    // then
    Assert.assertNull("Object not deleted", replicator.store_.get(ID_1.toString()));
  }

  @Test
  public void shouldIndexStoredFile() throws Exception {
    // given
    ReplicatorWrapper replicator = startNewReplicator();
    // when
    storeObject(replicator, ID_1, CONTENT_1);
    // then
    Set<String> index = replicator.getIndex();
    Assert.assertEquals("Incorrect index size", 1, index.size());
    Assert.assertTrue("Incorrect index content", index.contains(ID_1.toString()));
  }

  @Test
  public void shouldNotIndexDeletedFile() throws Exception {
    // given
    InMemoryStore<byte[]> store = new InMemoryStore<byte[]>();
    ReplicatorWrapper replicator = startNewReplicator(store);
    storeObject(replicator, ID_1, CONTENT_1);
    replicator = startNewReplicator(store);
    storeObject(replicator, ID_2, CONTENT_2);
    // when
    replicator = startNewReplicator(store);
    deleteObject(replicator, ID_1);
    // then
    Set<String> index = replicator.getIndex();
    Assert.assertEquals("Incorrect index size", 1, index.size());
    Assert.assertTrue("Incorrect index content", index.contains(ID_2.toString()));
  }



  // waits for the replicator thread to end
  private void storeObject(ReplicatorWrapper replicator, ObjectId id, String content)
      throws InterruptedException {
    replicator.sendMsg(new QueryToStoreObjectMessage("1", null, id,
        new EncryptedObject(content.getBytes(Charsets.UTF_8)), new HashSet<String>(), "1"));
    Message reply = replicator.receiveMsg();
    Preconditions.checkArgument(reply instanceof ConfirmationMessage, "Incorrect msg type " +
        reply.getClass());
    replicator.sendMsg(new TransactionResultMessage("1", null, TransactionAnswer.COMMIT));
    endReplicator(replicator);
  }

  private void deleteObject(ReplicatorWrapper replicator, ObjectId id) throws InterruptedException {
    replicator.sendMsg(new DeleteObjectMessage("1", null, id, "1"));
    Message reply = replicator.receiveMsg();
    Preconditions.checkArgument(reply instanceof ConfirmationMessage, "Incorrect msg type " +
        reply.getClass());
    endReplicator(replicator);
  }

  private ReplicatorWrapper startNewReplicator() {
    ReplicatorWrapper wrapper = new ReplicatorWrapper(new InMemoryStore<byte[]>());
    wrapper.thread_.start();
    return wrapper;
  }

  private ReplicatorWrapper startNewReplicator(KeyValueStore<byte[]> store) {
    ReplicatorWrapper wrapper = new ReplicatorWrapper(store);
    wrapper.thread_.start();
    return wrapper;
  }

  private void endReplicator(ReplicatorWrapper wrapper) throws InterruptedException {
    wrapper.thread_.join();
  }



  static class ReplicatorWrapper {
    public KeyValueStore<byte[]> store_;
    public Replicator replicator_;
    public BlockingQueue<Message> inQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> outQueue_ = new LinkedBlockingQueue<Message>();
    public BlockingQueue<Message> networkQueue_ = new LinkedBlockingQueue<Message>();
    public Thread thread_;

    public ReplicatorWrapper(KeyValueStore<byte[]> store) {
      store_ = store;
      replicator_ = new ReplicatorImpl(store_);
      replicator_.setInQueue(inQueue_);
      replicator_.setOutQueue(outQueue_);
      replicator_.setNetworkQueue(networkQueue_);
      thread_ = new Thread(replicator_);
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

    public Set<String> getIndex() {
      return replicator_.getStoredObjectsIds();
    }
  }
}
