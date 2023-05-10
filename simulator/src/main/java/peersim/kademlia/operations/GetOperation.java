package peersim.kademlia.operations;

import java.math.BigInteger;

/** An extension of the FindOperation calss to represent the GetOperation. */
public class GetOperation extends FindOperation {

  Object value;

  /**
   * Constructs a new GetOperation object.
   *
   * @param srcNode the ID of the source node.
   * @param destNode the ID of the destination node to find.
   * @param timestamp the timestamp of the operation.
   */
  public GetOperation(BigInteger srcNode, BigInteger destNode, long timestamp) {
    super(srcNode, destNode, timestamp);
  }

  /**
   * Sets the value of the Get operation.
   *
   * @param value the value to be set.
   */
  public void setValue(Object value) {
    this.value = value;
  }

  /**
   * Gets the value of the Get operation.
   *
   * @return the value of the Get operation.
   */
  public Object getValue() {
    return value;
  }
}
