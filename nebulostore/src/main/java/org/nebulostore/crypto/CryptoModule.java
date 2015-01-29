package org.nebulostore.crypto;

import com.google.inject.Inject;

import org.apache.log4j.Logger;
import org.nebulostore.appcore.InstanceMetadata;
import org.nebulostore.appcore.exceptions.NebuloException;
import org.nebulostore.appcore.messaging.Message;
import org.nebulostore.appcore.messaging.MessageVisitor;
import org.nebulostore.appcore.modules.JobModule;
import org.nebulostore.communication.naming.CommAddress;
import org.nebulostore.crypto.messages.PublicKeyMessage;
import org.nebulostore.dht.messages.GetDHTMessage;
import org.nebulostore.dht.messages.ValueDHTMessage;
import org.nebulostore.dispatcher.JobInitMessage;


/**
 * @author lukaszsiczek
 */
public class CryptoModule extends JobModule {
  private static final Logger LOGGER = Logger.getLogger(CryptoModule.class);

  private final MessageVisitor<Void> visitor_ = new CryptoModuleVisitor();
  private final CommAddress myAddress_;
  private String sourceJobId_;

  @Inject
  public CryptoModule(CommAddress myAddress) {
    myAddress_ = myAddress;
  }

  public void setSourceJobId(String sourceJobId) {
    sourceJobId_ = sourceJobId;
  }

  protected class CryptoModuleVisitor extends MessageVisitor<Void> {
    public Void visit(JobInitMessage message) {
      networkQueue_.add(new GetDHTMessage(jobId_, myAddress_.toKeyDHT()));
      return null;
    }

    public Void visit(ValueDHTMessage message) {
      if (message.getValue().getValue() instanceof InstanceMetadata) {
        InstanceMetadata instanceMetadata = (InstanceMetadata) message.getValue().getValue();
        LOGGER.debug(instanceMetadata.toString());
        outQueue_.add(new PublicKeyMessage(sourceJobId_, instanceMetadata.getPublicKey()));
      } else {
        LOGGER.warn("Received wrong type of message from DHT");
      }
      return null;
    }
  }

  @Override
  protected void processMessage(Message message) throws NebuloException {
    message.accept(visitor_);
  }
}
