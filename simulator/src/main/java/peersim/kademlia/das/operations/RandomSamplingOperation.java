package peersim.kademlia.das.operations;

import java.math.BigInteger;
import peersim.kademlia.operations.Operation;

/**
 * This class represents a random sampling operation and offer the methods needed to maintain and
 * update the closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RandomSamplingOperation extends Operation {

  /**
   * defaul constructor
   *
   * @param srcNode Id of the node to find
   * @param destNode Id of the node to find
   * @param timestamp Id of the node to find
   */
  public RandomSamplingOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
  }
}
