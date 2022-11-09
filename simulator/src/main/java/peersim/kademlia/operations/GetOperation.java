package peersim.kademlia.operations;

import java.math.BigInteger;
import peersim.kademlia.Message;

public class GetOperation extends Operation {

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public GetOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, Message.MSG_GET, timestamp);
  }
}
