package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  protected int nrHops;

  protected ArrayList<BigInteger> returned;

  /** Timestamp stop Operation */
  protected long stopTime;

  /** Messages in the operation */
  protected List<Long> messages;
  /**
   * defaul constructor
   *
   * @param destNode ID of the node to find
   */
  public Operation(BigInteger srcNode, BigInteger dstNode, long timestamp) {
    this.timestamp = timestamp;
    this.destNode = dstNode;
    this.srcNode = srcNode;
    this.nrHops = 0;
    // set a new find ID
    operationId = OPERATION_ID_GENERATOR++;
    this.messages = new ArrayList<>();
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

  public void addMessage(long msgId) {
    messages.add(msgId);
  }

  public List<Long> getMessages() {
    return messages;
  }

  public String getMessagesString() {
    // this.messages = this.messages + messageId + "|";
    String msgs = "";
    for (Long msg : messages) {
      msgs += msg + "|";
    }
    return msgs;
  }

  public int getHops() {
    return nrHops;
  }

  public void addHops(int nrHops) {
    this.nrHops += nrHops;
  }

  public void increaseHops() {
    nrHops++;
  }

  public void setStopTime(long time) {
    this.stopTime = time;
  }

  public abstract Map<String, Object> toMap();
}
