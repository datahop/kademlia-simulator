package peersim.kademlia;

import java.math.BigInteger;

/**
 * This class represent a timeout event.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class Timeout extends SimpleEvent {

  /** Message Type: PING (used to verify that a node is still alive) */
  public static final int TIMEOUT = 100;

  /**
   * Message Type: Time waiting for new tickets have expired (used by ticket-based topic table to
   * choose the next advert to register among the submitted tickets for a topic)
   */
  public static final int TICKET_TIMEOUT = 101;

  public static final int REG_TIMEOUT = 102;

  /** The node wich failed to response */
  public BigInteger node;

  /** The id of the message sent to the node */
  public long msgID;

  /** The id of the operation in wich the message has been sent */
  public long opID;

  /** The topic for which a ticket has been expired */
  public Topic topic;

  public BigInteger nodeSrc;

  // ______________________________________________________________________________________________
  /**
   * Creates an empty message by using default values (message type = MSG_LOOKUP and <code>
   * new String("")</code> value for the body of the message)
   */
  public Timeout(BigInteger node, long msgID, long opID) {
    super(TIMEOUT);
    this.node = node;
    this.msgID = msgID;
    this.opID = opID;
  }

  /**
   * Creates an empty message by using default values (message type = MSG_LOOKUP and <code>
   * new String("")</code> value for the body of the message)
   */
  public Timeout(Topic topic) {
    super(TICKET_TIMEOUT);
    this.topic = topic;
  }

  public Timeout(Topic topic, BigInteger nodeSrc) {
    super(REG_TIMEOUT);
    this.topic = topic;
    this.nodeSrc = nodeSrc;
  }
}
