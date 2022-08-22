package peersim.kademlia;

import java.math.BigInteger;

/**
 * This class represent a timeout event.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RetryTimeout extends SimpleEvent {

  /** Message Type: PING (used to verify that a node is still alive) */
  public static final int RETRY = 200;

  /** /** The node wich failed to response */
  public BigInteger node;

  /** The topic for which a ticket has been expired */
  public Topic topic;

  public BigInteger nodeSrc;

  public RetryTimeout(Topic topic, BigInteger nodeSrc) {
    super(RETRY);
    this.topic = topic;
    this.nodeSrc = nodeSrc;
  }
}
