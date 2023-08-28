package peersim.gossipsub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import peersim.kademlia.SimpleEvent;

/**
 * Message class provide all functionalities to magage the various messages, principally LOOKUP
 * messages (messages from application level sender destinated to another application level).<br>
 * Types Of messages:<br>
 * (application messages)<br>
 * - MSG_LOOKUP: indicates that the body Object containes information to application level of the
 * recipient<br>
 * <br>
 * (service internal protocol messages)<br>
 * - MSG_JOINREQUEST: message containing a join request of a node, the message is passed between
 * many pastry nodes accorting to the protocol<br>
 * - MSG_JOINREPLY: according to protocol, the body transport information related to a join reply
 * message <br>
 * - MSG_LSPROBEREQUEST:according to protocol, the body transport information related to a probe
 * request message <br>
 * - MSG_LSPROBEREPLY: not used in the current implementation<br>
 * - MSG_SERVICEPOLL: internal message used to provide cyclic cleaning service of dead nodes<br>
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
// ______________________________________________________________________________________
public class Message extends SimpleEvent {

  /** Internal generator for unique message IDs */
  private static long ID_GENERATOR = 0;

  /** Message Type: PING (used to verify that a node is still alive) */
  public static final int MSG_EMPTY = 0;

  public static final int MSG_GRAFT = 1;

  public static final int MSG_PRUNE = 2;

  public static final int MSG_IHAVE = 3;

  public static final int MSG_IWANT = 4;

  public static final int MSG_JOIN = 5;

  public static final int MSG_LEAVE = 6;

  public static final int MSG_PUBLISH = 7;

  public static final int MSG_MESSAGE = 8;

  public static final int MSG_INIT_NEW_BLOCK = 10;

  // ______________________________________________________________________________________________
  /** This Object contains the body of the message, no matter what it contains */
  public Object body = null;

  // ______________________________________________________________________________________________
  /** This Object contains the body of the message, no matter what it contains */
  public Object value = null;

  /** ID of the message. this is automatically generated univocally, and should not change */
  public long id;

  /** ACK number of the message. This is in the response message. */
  public long ackId;

  /** Id of the search operation */
  public long operationId;

  /** Recipient node of the message */
  public GossipNode dst;

  /** Source node of the message: has to be filled at application level */
  public GossipNode src;

  /** Available to count the number of hops the message did. */
  public int nrHops = 0;

  // ______________________________________________________________________________________________
  /**
   * Creates an empty message by using default values (message type = MSG_LOOKUP and <code>
   * new String("")</code> value for the body of the message)
   */
  public Message() {
    this(MSG_EMPTY, "");
  }

  /**
   * Create a message with specific type and empty body
   *
   * @param messageType int type of the message
   */
  public Message(int messageType) {
    this(messageType, "");
  }

  // ______________________________________________________________________________________________
  /**
   * Creates a message with specific type and body
   *
   * @param messageType int type of the message
   * @param body Object body to assign (shallow copy)
   */
  public Message(int messageType, Object body) {
    super(messageType);
    this.id = (ID_GENERATOR++);
    this.body = body;
  }

  // ______________________________________________________________________________________________
  /**
   * Creates a message with specific type and body
   *
   * @param messageType int type of the message
   * @param body Object body to assign (shallow copy)
   */
  public Message(int messageType, Object body, Object value) {
    super(messageType);
    this.id = (ID_GENERATOR++);
    this.body = body;
    this.value = value;
  }
  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a find value request
   *
   * @param body Object
   * @return Message
   */
  /*public static final Message makeFindValue(Object body) {
  	return new Message(MSG_FINDVALUE, body);
  }*/

  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a find value request
   *
   * @param body Object
   * @return Message
   */
  /*public static final Message makeJoinRequest(Object body) {
  	return new Message(MSG_FINDVALUE, body);
  }*/

  // ______________________________________________________________________________________________
  public String toString() {
    String s = "[ID=" + id + "][DEST=" + dst + "]";
    return s + "[Type=" + typeToString() + "] BODY=(...)";
  }

  // ______________________________________________________________________________________________
  public Message copy() {
    Message dolly = new Message();
    dolly.type = this.type;
    dolly.src = this.src;
    dolly.dst = this.dst;
    dolly.operationId = this.operationId;
    dolly.body = this.body; // deep cloning?
    dolly.value = this.value;
    return dolly;
  }

  public static final Message makeInitJoinMessage(Object body) {
    return new Message(MSG_JOIN, body);
  }

  public static final Message makeGraftMessage(String topic) {
    return new Message(MSG_GRAFT, topic);
  }

  public static final Message makeIHaveMessage(String topic, List<BigInteger> ids) {
    return new Message(MSG_IHAVE, topic, ids);
  }

  public static final Message makeIWantMessage(String topic, List<BigInteger> ids) {
    return new Message(MSG_IWANT, topic, ids);
  }

  public static final Message makePruneMessage(String topic) {
    return new Message(MSG_PRUNE, topic);
  }

  public static final Message makePublishMessage(String topic, Object value) {
    return new Message(MSG_PUBLISH, topic, value);
  }

  public static final Message makeMessage(String topic, Object value) {
    return new Message(MSG_MESSAGE, topic, value);
  }

  public static final Message makeInitNewBlock(Object body) {
    return new Message(MSG_INIT_NEW_BLOCK, body);
  }

  public static final Message makeLeaveMessage(Object body) {
    return new Message(MSG_LEAVE, body);
  }
  // ______________________________________________________________________________________________
  public String typeToString() {
    switch (type) {
      case MSG_EMPTY:
        return "MSG_EMPTY";
      case MSG_JOIN:
        return "MSG_JOIN";
      case MSG_GRAFT:
        return "MSG_GRAFT";
      case MSG_IHAVE:
        return "MSG_IHAVE";
      case MSG_IWANT:
        return "MSG_IWANT";
      case MSG_PRUNE:
        return "MSG_PRUNE";
      case MSG_PUBLISH:
        return "MSG_PUBLISH";
      case MSG_MESSAGE:
        return "MSG_MESSAGE";
      case MSG_LEAVE:
        return "MSG_LEAVE";
      default:
        return "UNKNOW:" + type;
    }
  }

  public Map<String, Object> toMap(boolean sent) {
    Map<String, Object> result = new HashMap<String, Object>();
    result.put("id", this.id);
    result.put("type", this.typeToString());
    result.put("src", this.src.getId());
    result.put("dst", this.dst.getId());
    if (sent) {
      result.put("status", "sent");
    } else {
      result.put("status", "received");
    }
    result.put("time", this.timestamp);
    return result;
  }
}
