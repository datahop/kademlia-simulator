package peersim.kademlia.operations;

import java.math.BigInteger;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set. It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class PutOperation extends GetOperation {

  /**
   * Default constructor
   *
   * @param srcNode ID of the node that stores the value
   * @param value the value to store
   * @param timestamp the timestamp of the operation
   */
  public PutOperation(BigInteger srcNode, BigInteger value, long timestamp) {
    super(srcNode, value, timestamp);
  }
}
