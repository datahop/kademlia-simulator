package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import peersim.kademlia.SimpleEvent;

/**
 * FindLogging class provide all functionalities to magage a find operation and report it to the
 * Observer
 */
// ______________________________________________________________________________________
public class OpLogging extends SimpleEvent {

  public static final int OP_FIND = 5;
  public static final int FIND_DIST = 6;
  public static final int OP_GET = 8;
  public static final int OP_PUT = 9;

  /** ID of the actual find operation. */
  public long id;

  /** Timestamp start Operation */
  public long start;

  /** Timestamp stop Operation */
  public long stop;

  /** Messages in the operation */
  public String messages;

  /** Source node of the message: has to be filled at application level */
  public BigInteger src;

  public Boolean finish;

  public int type = 0;

  public OpLogging() {
    this.messages = "";
  }

  public OpLogging(long id, BigInteger src, long time, int type) {
    this.id = id;
    this.messages = "";
    this.start = time;
    this.stop = 0;
    this.finish = false;
    this.src = src;
    this.type = type;
  }
  // ______________________________________________________________________________________________

  public void AddMessage(long messageId) {
    this.messages = this.messages + messageId + "|";
  }

  public void SetStop(long time) {
    this.stop = time;
  }

  public long getId() {
    return this.id;
  }

  public Boolean isFinished() {
    return this.finish;
  }

  public Boolean SetFinished() {
    return this.finish;
  }

  public String typeToString() {
    switch (type) {
      case OP_FIND:
        return "OP_FIND";
      case FIND_DIST:
        return "OP_FIND";
      case OP_GET:
        return "OP_GET";
      case OP_PUT:
        return "OP_PUT";
      default:
        return "UNKNOW:" + type;
    }
  }

  public Map<String, Object> toMap() {
    Map<String, Object> result = new HashMap<String, Object>();

    result.put("id", this.id);
    result.put("src", this.src);
    result.put("type", this.typeToString());
    result.put("messages", this.messages);
    result.put("start", this.start);
    result.put("stop", this.stop);

    return result;
  }
}
