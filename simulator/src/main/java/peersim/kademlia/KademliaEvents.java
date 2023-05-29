package peersim.kademlia;

import java.math.BigInteger;
import peersim.kademlia.operations.Operation;

/**
 * Interface for events triggered by Kademlia operations. defines two methods for handling events:
 * nodesFound() and operationComplete().
 */
public interface KademliaEvents {

  /**
   * Callback method triggered when the operation finds neighboring nodes.
   *
   * @param op the operation that triggered the event
   * @param neighbours an array of BigIntegers representing neighboring nodes
   */
  public void nodesFound(Operation op, BigInteger[] neighbours);

  /**
   * Callback method triggered when the operation is complete.
   *
   * @param op the operation that triggered the event
   */
  public void operationComplete(Operation op);

  public void putValueReceived(Object o);
}
