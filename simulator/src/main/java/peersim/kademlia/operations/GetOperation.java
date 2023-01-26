package peersim.kademlia.operations;

import java.math.BigInteger;

public class GetOperation extends FindOperation {

  Object value;
  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public GetOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
  }

  /**
   * Save found value in get operation
   *
   * @param value
   */
  public void setValue(Object value) {
    this.value = value;
  }

  /** Get found value in get operation */
  public Object getValue() {
    return value;
  }
}
