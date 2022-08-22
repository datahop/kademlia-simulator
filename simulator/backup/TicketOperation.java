package peersim.kademlia.operations;

import java.math.BigInteger;
import peersim.kademlia.Message;
import peersim.kademlia.Topic;

/**
 * This class represents a ticket request operation and offer the methods needed to maintain and
 * update the closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 */
public class TicketOperation extends Operation {

  private Topic topic;

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public TicketOperation(BigInteger srcNode, long timestamp, Topic t) {
    super(srcNode, t.getTopicID(), Message.MSG_TICKET_REQUEST, timestamp);
    this.topic = t;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
