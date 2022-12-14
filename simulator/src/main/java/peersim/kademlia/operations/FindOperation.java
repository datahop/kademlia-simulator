package peersim.kademlia.operations;

import java.math.BigInteger;
import peersim.kademlia.Message;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class FindOperation extends Operation {

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public FindOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, Message.MSG_FIND, timestamp);
  }
}
