package peersim.kademlia.discv5;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.logging.Logger;
import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.Message;
import peersim.kademlia.RoutingTable;
import peersim.kademlia.SimpleEvent;
import peersim.kademlia.Timeout;
import peersim.kademlia.Util;
import peersim.transport.UnreliableTransport;

public class Discv5Protocol implements Cloneable, EDProtocol {

  private static final String PAR_TRANSPORT = "transport";
  private static final String PAR_KADEMLIA = "kademlia";

  private static String prefix = null;
  private UnreliableTransport transport;
  private int tid;
  private int protocolId;
  // private int kademliaId;
  /** trace message sent for timeout purpose */
  private TreeMap<Long, Long> sentMsg;

  private KademliaProtocol kadProtocol;
  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INSTALLED = false;
  // Target topic for attackers
  protected Topic targetTopic = null;

  private Logger logger;

  /** Table to keep track of topic registrations */
  protected HashMap<BigInteger, TicketTable> ticketTables;

  /** Table to search for topics */
  protected HashMap<BigInteger, SearchTable> searchTables;

  protected HashSet<String> activeTopics;

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5Protocol dolly = new Discv5Protocol(Discv5Protocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public Discv5Protocol(String prefix) {

    Discv5Protocol.prefix = prefix;

    ticketTables = new HashMap<BigInteger, TicketTable>();
    searchTables = new HashMap<BigInteger, SearchTable>();

    _init();

    sentMsg = new TreeMap<Long, Long>();

    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

    activeTopics = new HashSet<String>();

    // kademliaId = Configuration.getPid(prefix + "." + PAR_KADEMLIA);
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  private void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    _ALREADY_INSTALLED = true;
  }

  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  public void processEvent(Node myNode, int myPid, Object event) {

    // Parse message content Activate the correct event manager fot the particular event
    this.protocolId = myPid;

    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.getKademliaProtocol().getNode();
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegisterTopic(m, myPid);
        break;

      case Message.MSG_INIT_TOPIC_LOOKUP:
        m = (Message) event;
        handleInitTopicLookup(m, myPid);
        break;
    }
  }

  public void setKademliaProtocol(KademliaProtocol prot) {
    this.kadProtocol = prot;
    this.logger = prot.getLogger();
  }

  public KademliaProtocol getKademliaProtocol() {
    // System.out.println(
    //    "getKademliaProtocol " + kademliaId + " " + (Network.prototype).getProtocol(kademliaId));
    // return (KademliaProtocol) (Network.prototype).getProtocol(kademliaId);
    return kadProtocol;
  }

  /** Getter for target topic when a protocol is malicious */
  protected Topic getTargetTopic() {
    return this.targetTopic;
  }

  /**
   * Setter for target topic when a protocol is malicious
   *
   * @param t Topic that is target for attack by this protocol instance
   */
  protected void setTargetTopic(Topic t) {
    this.targetTopic = t;
  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitTopicLookup(Message m, int myPid) {
    Topic t = (Topic) m.body;

    logger.warning(
        "Send init lookup for topic "
            + this.getKademliaProtocol().getNode().getId()
            + " "
            + t.getTopic());
  }

  /**
   * Start a register topic opearation.<br>
   * If this is an on-going register operation with a previously obtained ticket, then send a
   * REGTOPIC message; otherwise, Find the ALPHA closest node and send REGTOPIC message to them
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitRegisterTopic(Message m, int myPid) {
    Topic t = (Topic) m.body;

    logger.warning(
        "handleInitRegisterTopic "
            + t.getTopic()
            + " "
            + t.getTopicID()
            + " "
            + KademliaCommonConfig.TICKET_BUCKET_SIZE
            + " "
            + this.getKademliaProtocol().getNode().isEvil());
    // restore the IF statement
    // KademliaObserver.addTopicRegistration(t, this.getKademliaProtocol().getNode().getId());

    activeTopics.add(t.getTopic());

    TicketTable tt =
        new TicketTable(
            KademliaCommonConfig.TTNBUCKETS,
            KademliaCommonConfig.TICKET_BUCKET_SIZE,
            KademliaCommonConfig.TICKET_TABLE_REPLACEMENTS,
            this,
            t,
            myPid,
            KademliaCommonConfig.TICKET_REFRESH == 1);
    tt.setNodeId(t.getTopicID());
    ticketTables.put(t.getTopicID(), tt);

    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = kadProtocol.getRoutingTable().getNeighbours(i);
      tt.addNeighbour(neighbours);
    }
  }

  public void sendTicketRequest(BigInteger dest, Topic t, int myPid) {
    logger.info("Sending ticket request to " + dest + " for topic " + t.topic);
    /*TicketOperation top = new TicketOperation(this.node.getId(), CommonState.getTime(), t);
        top.body = t;
        operations.put(top.operationId, top);

        // Lookup the target address in the routing table
        BigInteger[] neighbours = new BigInteger[] {dest};

        top.elaborateResponse(neighbours);
        top.available_requests = KademliaCommonConfig.ALPHA;

        Message m = new Message(Message.MSG_TICKET_REQUEST, t);

        m.timestamp = CommonState.getTime();
        // set message operation id
        m.operationId = top.operationId;
        m.src = this.node;

        // logger.warning("Send ticket request to " + dest + " for topic " + t.getTopic());
        sendMessage(m, top.getNeighbour(), myPid);
        // if (KademliaCommonConfig.PARALLELREGISTRATIONS == 0)
        //	ticketTables.get(t.topicID).decreaseAvailableRequests();
        // System.out.println("available_requests:" + tt.getAvailableRequests());
    */
  }

  // public void refreshBucket(TicketTable rou, BigInteger node, int distance) {
  public void refreshBucket(RoutingTable rou, int distance) {

    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = kadProtocol.getRoutingTable().getNeighbours(i);
      // logger.warning("RefreshBucket "+neighbours.length+" "+i+" "+distance);
      for (BigInteger node : neighbours) {
        if (Util.distance(rou.getNodeId(), node) == distance) {
          // logger.warning("RefreshBucket add neighbour "+node);
          if (rou instanceof TicketTable) ((TicketTable) rou).addNeighbour(node);
          if (rou instanceof SearchTable) ((SearchTable) rou).addNeighbour(node);
        }
      }
    }
  }

  /**
   * schedule sending a message after a given delay with current transport layer and starting the
   * timeout timer (which is an event) if the message is a request
   *
   * @param m the message to send
   * @param destId the Id of the destination node
   * @param myPid the sender Pid
   * @param delay the delay to wait before sending
   */
  public void scheduleSendMessage(Message m, BigInteger destId, int myPid, long delay) {
    Node src = Util.nodeIdtoNode(this.getKademliaProtocol().getNode().getId(), myPid);
    Node dest = Util.nodeIdtoNode(destId, myPid);

    assert delay >= 0 : "attempting to schedule a message in the past";

    m.src = this.getKademliaProtocol().getNode();
    m.dest = Util.nodeIdtoNode(destId, myPid).getKademliaProtocol().getNode();

    logger.info("-> (" + m + "/" + m.id + ") " + destId);

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    long network_delay = transport.getLatency(src, dest);

    EDSimulator.add(network_delay + delay, m, dest, myPid);
    if ((m.getType() == Message.MSG_FIND)
        || (m.getType() == Message.MSG_REGISTER)
        || (m.getType() == Message.MSG_TICKET_REQUEST)) {

      Timeout t = new Timeout(destId, m.id, m.operationId);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(delay + 4 * network_delay, t, src, myPid);
    }
  }
}
