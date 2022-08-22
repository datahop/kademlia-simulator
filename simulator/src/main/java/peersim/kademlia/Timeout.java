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

  /** The node wich failed to response */
  public BigInteger node;

  /** The id of the message sent to the node */
  public long msgID;

  /** The id of the operation in wich the message has been sent */
  public long opID;

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
}
