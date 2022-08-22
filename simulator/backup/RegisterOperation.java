package peersim.kademlia.operations;

import java.math.BigInteger;
import peersim.kademlia.Message;
import peersim.kademlia.Topic;
import peersim.kademlia.TopicRegistration;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RegisterOperation extends Operation {

  public Topic topic;
  private TopicRegistration registration; // FIXME: is this variable used?

  private Message message;
  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public RegisterOperation(BigInteger srcNode, long timestamp, Topic t, TopicRegistration r) {

    super(srcNode, t.getTopicID(), Message.MSG_REGISTER, timestamp);

    this.registration = r;
    this.topic = t;
  }

  public RegisterOperation(BigInteger srcNode, long timestamp, Topic t, BigInteger targetAddr) {
    super(srcNode, targetAddr, Message.MSG_REGISTER, timestamp);
    this.topic = t;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public void setMessage(Message message) {
    this.message = message;
  }

  public Message getMessage() {
    return this.message;
  }

  public Topic getTopic() {
    return this.topic;
  }
}
