package peersim.kademlia.operations;

import java.math.BigInteger;

public class GetOperation extends FindOperation {

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public GetOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
  }
}
