package peersim.kademlia;

import java.util.HashMap;
import java.util.Map;

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

  /** Message Type: STORE (Stores a (key, value) pair in one node) */
  public static final int MSG_STORE = 1;

  /** Message Type: INIT_FIND (command to a node to start looking for a node) */
  public static final int MSG_INIT_FIND = 2;

  /** Message Type: INIT_GET (command to a node to start looking for a value) */
  public static final int MSG_INIT_GET = 3;

  /** Message Type: INIT_PUT (command to a node to storing a value) */
  public static final int MSG_INIT_PUT = 4;

  /** Message Type: FINDVALUE (message regarding value find) */
  public static final int MSG_FIND = 5;

  /** Message Type: FINDVALUE (message regarding value find) finding by distance */
  public static final int MSG_FIND_DIST = 6;

  /** Message Type: RESPONSE (respons message to a findvalue or findnode) */
  public static final int MSG_RESPONSE = 7;

  /** Message Type: FINDVALUE (message regarding value find) finding by distance */
  public static final int MSG_PUT = 8;

  /** Message Type: RESPONSE (respons message to a findvalue or findnode) */
  public static final int MSG_GET = 9;

  // DISv5 specific messages
  /** Message Type: REGISTER (register the node under a topic) */
  public static final int MSG_INIT_NEW_BLOCK = 10;

  public static final int MSG_INIT_GET_SAMPLE = 11;

  public static final int MSG_GET_SAMPLE = 12;

  public static final int MSG_GET_SAMPLE_RESPONSE = 13;

  public static final int MSG_GET_ANY_SAMPLE = 14;

  public static final int MSG_GET_ANY_SAMPLE_RESPONSE = 15;

  /**
   * Message Type: INIT_FIND_REGION_BASED (command to a node to start looking for node within a
   * region)
   */
  public static final int MSG_INIT_FIND_REGION_BASED = 16;

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
  public KademliaNode dst;

  /** Source node of the message: has to be filled at application level */
  public KademliaNode src;

  /** Available to count the number of hops the message did. */
  protected int nrHops = 0;

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
   * Encapsulates the creation of a find node request
   *
   * @param body Object
   * @return Message
   */
  public static final Message makeInitFindNode(Object body) {
    return new Message(MSG_INIT_FIND, body);
  }
  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a region-based find node request
   *
   * @param body Object
   * @return Message
   */
  public static final Message makeInitRegionBasedFindNode(Object body, Object value) {
    return new Message(MSG_INIT_FIND_REGION_BASED, body, value);
  }

  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a GET request
   *
   * @param body Object
   * @return Message
   */
  public static final Message makeInitGetValue(Object body) {
    return new Message(MSG_INIT_GET, body);
  }

  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a PUT request
   *
   * @param body Object
   * @return Message
   */
  public static final Message makeInitPutValue(Object body, Object value) {
    return new Message(MSG_INIT_PUT, body, value);
  }

  // ______________________________________________________________________________________________
  /**
   * Encapsulates the creation of a PUT request
   *
   * @param body Object
   * @return Message
   */
  public static final Message makeInitNewBlock(Object body) {
    return new Message(MSG_INIT_NEW_BLOCK, body);
  }

  public static final Message makeInitGetSample(Object body) {
    return new Message(MSG_INIT_GET_SAMPLE, body);
  }

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

    return dolly;
  }

  // ______________________________________________________________________________________________
  public String typeToString() {
    switch (type) {
      case MSG_EMPTY:
        return "MSG_EMPTY";
      case MSG_STORE:
        return "MSG_STORE";
      case MSG_INIT_FIND:
        return "MSG_INIT_FIND";
      case MSG_FIND:
        return "MSG_FIND";
      case MSG_FIND_DIST:
        return "MSG_FIND_DIST";
      case MSG_RESPONSE:
        return "MSG_RESPONSE";
      case MSG_INIT_NEW_BLOCK:
        return "MSG_INIT_NEW_BLOCK";
      case MSG_INIT_FIND_REGION_BASED:
        return "MSG_INIT_REGION_BASED_FIND";
      case MSG_GET:
        return "MSG_GET";
      case MSG_PUT:
        return "MSG_PUT";
      case MSG_INIT_GET_SAMPLE:
        return "MSG_INIT_GET_SAMPLE";
      case MSG_GET_SAMPLE:
        return "MSG_GET_SAMPLE";
      case MSG_GET_SAMPLE_RESPONSE:
        return "MSG_GET_SAMPLE_RESPONSE";
      case MSG_GET_ANY_SAMPLE:
        return "MSG_GET_ANY_SAMPLE";
      case MSG_GET_ANY_SAMPLE_RESPONSE:
        return "MSG_GET_ANY_SAMPLE_RESPONSE";

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
