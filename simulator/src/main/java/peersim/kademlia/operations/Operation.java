package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;

/**
 * This class represents a find operation and offer the methods needed to maintain and update the
 * closest set.<br>
 * It also maintains the number of parallel requsts that can has a maximum of ALPHA.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public abstract class Operation {

  /** unique sequence number generator of the operation */
  protected static long OPERATION_ID_GENERATOR = 0;

  protected boolean finished = false;
  /** represent univocally the find operation */
  protected long operationId;

  /** ID of the node to find */
  protected BigInteger destNode;

  /** ID of the node initiating the operation */
  protected BigInteger srcNode;

  /** Body of the original find message */
  protected Object body;

  /** Start timestamp of the search operation */
  protected long timestamp = 0;

  /** Number of hops the message did */
  public int nrHops = 0;

  protected ArrayList<BigInteger> returned;

  /**
   * defaul constructor
   *
   * @param destNode Id of the node to find
   */
  public Operation(BigInteger srcNode, BigInteger dstNode, long timestamp) {
    this.timestamp = timestamp;
    this.destNode = dstNode;
    this.srcNode = srcNode;

    // set a new find id
    operationId = OPERATION_ID_GENERATOR++;

    returned = new ArrayList<BigInteger>();
  }

  public void setBody(Object body) {
    this.body = body;
  }

  public Object getBody() {
    return body;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public BigInteger getDestNode() {
    return destNode;
  }

  public long getId() {
    return operationId;
  }

  public boolean isFinished() {
    return this.finished;
  }

  public void setFinished(boolean finished) {
    this.finished = finished;
  }
}
