package peersim.kademlia;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class.<br>
 * See the Kademlia bibliografy for more information about the protocol.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
import java.math.BigInteger;
import java.util.Arrays;
// logging
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.transport.UnreliableTransport;

// __________________________________________________________________________________________________
public class KademliaProtocol implements Cloneable, EDProtocol {

  // VARIABLE PARAMETERS
  final String PAR_K = "K";
  final String PAR_ALPHA = "ALPHA";
  final String PAR_BITS = "BITS";
  final String PAR_FINDMODE = "FINDMODE";

  private static final String PAR_TRANSPORT = "transport";
  private static String prefix = null;
  private UnreliableTransport transport;
  private int tid;
  private int kademliaid;

  /** allow to call the service initializer only once */
  private static boolean _ALREADY_INSTALLED = false;

  /** routing table of this pastry node */
  private RoutingTable routingTable;

  /** trace message sent for timeout purpose */
  private TreeMap<Long, Long> sentMsg;

  /** find operations set */
  private LinkedHashMap<Long, FindOperation> findOp;

  /** Kademlia node instance */
  private KademliaNode node;

  /** logging handler */
  protected Logger logger;

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    KademliaProtocol dolly = new KademliaProtocol(KademliaProtocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public KademliaProtocol(String prefix) {

    this.node = null; // empty nodeId
    KademliaProtocol.prefix = prefix;

    _init();

    routingTable =
        new RoutingTable(
            KademliaCommonConfig.NBUCKETS,
            KademliaCommonConfig.K,
            KademliaCommonConfig.MAXREPLACEMENT);

    sentMsg = new TreeMap<Long, Long>();

    findOp = new LinkedHashMap<Long, FindOperation>();

    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  private void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    // read paramaters
    KademliaCommonConfig.K = Configuration.getInt(prefix + "." + PAR_K, KademliaCommonConfig.K);
    KademliaCommonConfig.ALPHA =
        Configuration.getInt(prefix + "." + PAR_ALPHA, KademliaCommonConfig.ALPHA);
    KademliaCommonConfig.BITS =
        Configuration.getInt(prefix + "." + PAR_BITS, KademliaCommonConfig.BITS);

    KademliaCommonConfig.FINDMODE =
        Configuration.getInt(prefix + "." + PAR_FINDMODE, KademliaCommonConfig.FINDMODE);

    _ALREADY_INSTALLED = true;
  }

  /**
   * Perform the required operation upon receiving a message in response to a ROUTE message.<br>
   * Update the find operation record with the closest set of neighbour received. Than, send as many
   * ROUTE request I can (according to the ALPHA parameter).<br>
   * If no closest neighbour available and no outstanding messages stop the find operation.
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  private void handleResponse(Message m, int myPid) {
    // add message source to my routing table
    if (m.src != null) {
      routingTable.addNeighbour(m.src.getId());
    }

    // get corresponding find operation (using the message field operationId)
    FindOperation fop = this.findOp.get(m.operationId);

    if (fop != null) {
      // save received neighbour in the closest Set of fin operation
      try {
        fop.elaborateResponse((BigInteger[]) m.body);
      } catch (Exception ex) {
        fop.available_requests++;
      }

      BigInteger[] neighbours = (BigInteger[]) m.body;
      for (BigInteger neighbour : neighbours) routingTable.addNeighbour(neighbour);

      if (!fop.isFinished() && Arrays.asList(neighbours).contains(fop.destNode)) {
        logger.warning("Found node " + fop.destNode);

        KademliaObserver.find_ok.add(1);
        fop.setFinished(true);
        return;
      }

      while (fop.available_requests > 0) { // I can send a new find request

        // get an available neighbour
        BigInteger neighbour = fop.getNeighbour();

        if (neighbour != null) {
          // create a new request to send to neighbour
          Message request = new Message(Message.MSG_FIND);
          if (KademliaCommonConfig.FINDMODE == 1) request = new Message(Message.MSG_FIND_DIST);
          request.operationId = m.operationId;
          request.src = this.getNode();
          request.dest = m.dest;
          request.body = fop.destNode;
          if (KademliaCommonConfig.FINDMODE == 1)
            request.body = Util.distance(fop.destNode, (BigInteger) fop.body);
          // increment hop count
          fop.nrHops++;

          // send find request
          sendMessage(request, neighbour, myPid);
        } else if (fop.available_requests
            == KademliaCommonConfig.ALPHA) { // no new neighbour and no outstanding requests
          // search operation finished
          findOp.remove(fop.operationId);

          if (fop.body.equals("Automatically Generated Traffic")
              && fop.closestSet.containsKey(fop.destNode)) {
            // update statistics
            long timeInterval = (CommonState.getTime()) - (fop.timestamp);
            KademliaObserver.timeStore.add(timeInterval);
            KademliaObserver.hopStore.add(fop.nrHops);
            KademliaObserver.msg_deliv.add(1);
          }

          return;

        } else { // no neighbour available but exists oustanding request to wait
          return;
        }
      }
    } else {
      System.err.println("There has been some error in the protocol");
    }
  }

  /**
   * Response to a route request.<br>
   * Find the ALPHA closest node consulting the k-buckets and return them to the sender.
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  private void handleFind(Message m, int myPid) {
    // get the ALPHA closest node to destNode

    logger.info("handleFind received");

    BigInteger[] neighbours = new BigInteger[KademliaCommonConfig.K];
    if (m.getType() == Message.MSG_FIND) {
      neighbours = this.routingTable.getNeighbours((BigInteger) m.body, m.src.getId());
    } else if (m.getType() == Message.MSG_FIND_DIST) {
      neighbours = this.routingTable.getNeighbours((int) m.body);
    }
    // for (BigInteger neigh : neighbours) logger.warning("Neighbours " + neigh);
    // create a response message containing the neighbours (with the same id of the request)
    Message response = new Message(Message.MSG_RESPONSE, neighbours);
    response.operationId = m.operationId;
    response.dest = m.dest;
    response.src = this.getNode();
    response.ackId = m.id; // set ACK number

    // send back the neighbours to the source of the message
    sendMessage(response, m.src.getId(), myPid);
  }

  /**
   * Start a find node opearation.<br>
   * Find the ALPHA closest node and send find request to them.
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitFind(Message m, int myPid) {

    logger.info("handleInitFind " + (BigInteger) m.body);
    KademliaObserver.find_op.add(1);

    // create find operation and add to operations array
    // FindOperation fop = new FindOperation(m.dest, m.timestamp);
    FindOperation fop = new FindOperation((BigInteger) m.body, m.timestamp);

    fop.body = m.body;
    findOp.put(fop.operationId, fop);

    // get the ALPHA closest node to srcNode and add to find operation
    BigInteger[] neighbours =
        this.routingTable.getNeighbours((BigInteger) m.body, this.getNode().getId());
    fop.elaborateResponse(neighbours);
    fop.available_requests = KademliaCommonConfig.ALPHA;

    // set message operation id
    m.operationId = fop.operationId;

    m.src = this.getNode();

    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = fop.getNeighbour();
      if (nextNode != null) {
        m.dest =
            Util.nodeIdtoNode(nextNode, myPid)
                .getKademliaProtocol()
                .getNode(); // new KademliaNode(nextNode);
        // set message type depending on find mode
        if (KademliaCommonConfig.FINDMODE == 0) m.type = Message.MSG_FIND;
        else {
          m.type = Message.MSG_FIND_DIST;
          m.body = Util.distance(nextNode, (BigInteger) fop.body);
        }

        logger.info("sendMessage to " + nextNode);

        sendMessage(m.copy(), nextNode, myPid);
        fop.nrHops++;
      }
    }
  }

  /**
   * send a message with current transport layer and starting the timeout timer (wich is an event)
   * if the message is a request
   *
   * @param m the message to send
   * @param destId the Id of the destination node
   * @param myPid the sender Pid
   */
  public void sendMessage(Message m, BigInteger destId, int myPid) {
    // add destination to routing table
    this.routingTable.addNeighbour(destId);
    // int destpid;
    assert m.src != null;
    assert m.dest != null;

    Node src = Util.nodeIdtoNode(this.getNode().getId(), myPid);
    Node dest = Util.nodeIdtoNode(destId, myPid);

    // destpid = dest.getKademliaProtocol().getProtocolID();

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    transport.send(src, dest, m, kademliaid);

    if (m.getType() == Message.MSG_FIND || m.getType() == Message.MSG_FIND_DIST) { // is a request
      Timeout t = new Timeout(destId, m.id, m.operationId);
      long latency = transport.getLatency(src, dest);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(4 * latency, t, src, myPid); // set delay = 2*RTT
    }
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
    this.kademliaid = myPid;

    Message m;

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_RESPONSE:
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleResponse(m, myPid);
        break;

      case Message.MSG_INIT_FIND:
        m = (Message) event;
        handleInitFind(m, myPid);
        break;

      case Message.MSG_FIND:
      case Message.MSG_FIND_DIST:
        m = (Message) event;
        handleFind(m, myPid);
        break;

      case Message.MSG_EMPTY:
        // TO DO
        break;

      case Message.MSG_STORE:
        // TO DO
        break;

        /*case Timeout.TIMEOUT: // timeout
        Timeout t = (Timeout) event;
        if (sentMsg.containsKey(t.msgID)) { // the response msg isn't arrived
          // remove form sentMsg
          sentMsg.remove(t.msgID);
          // remove node from my routing table
          this.routingTable.removeNeighbour(t.node);
          // remove from closestSet of find operation
          this.findOp.get(t.opID).closestSet.remove(t.node);
          // try another node
          Message m1 = new Message();
          m1.operationId = t.opID;
          m1.src = getNode();
          m1.dest = this.findOp.get(t.opID).destNode;
          this.handleResponse(m1, myPid);
        }
        break;*/
    }
  }

  /** get the current Node */
  public KademliaNode getNode() {
    return this.node;
  }

  /** get the kademlia node routing table */
  public RoutingTable getRoutingTable() {
    return this.routingTable;
  }

  /** Set the protocol ID for this node. */
  public void setProtocolID(int protocolID) {
    this.kademliaid = protocolID;
  }

  /**
   * set the current NodeId
   *
   * @param tmp BigInteger
   */
  public void setNode(KademliaNode node) {

    this.node = node;
    this.routingTable.setNodeId(node.getId());

    logger = Logger.getLogger(node.getId().toString());
    logger.setUseParentHandlers(false);
    ConsoleHandler handler = new ConsoleHandler();
    logger.setLevel(Level.WARNING);
    // logger.setLevel(Level.ALL);
    handler.setFormatter(
        new SimpleFormatter() {
          private static final String format = "[%d][%s] %3$s %n";

          @Override
          public synchronized String format(LogRecord lr) {
            return String.format(format, CommonState.getTime(), logger.getName(), lr.getMessage());
          }
        });
    logger.addHandler(handler);
  }

  public Logger getLogger() {
    return this.logger;
  }
}
