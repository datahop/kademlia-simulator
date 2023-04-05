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
import java.util.LinkedHashMap;
import java.util.Map;
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
import peersim.kademlia.operations.FindOperation;
import peersim.kademlia.operations.GetOperation;
import peersim.kademlia.operations.Operation;
import peersim.kademlia.operations.PutOperation;
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
  public KademliaNode node;

  /** logging handler */
  protected Logger logger;

  private KeyValueStore kv;

  private KademliaEvents callback;

  public LinkedHashMap<Long, OpLogging> findLog;

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

    findLog = new LinkedHashMap<Long, OpLogging>();

    tid = Configuration.getPid(prefix + "." + PAR_TRANSPORT);

    kv = new KeyValueStore();

    // System.out.println("New kademliaprotocol");
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
   * Search through the network the Node having a specific node Id, by performing binary serach (we
   * concern about the ordering of the network).
   *
   * @param searchNodeId BigInteger
   * @return Node
   */
  public Node nodeIdtoNode(BigInteger searchNodeId) {
    if (searchNodeId == null) return null;

    int inf = 0;
    int sup = Network.size() - 1;
    int m;

    while (inf <= sup) {
      m = (inf + sup) / 2;

      BigInteger mId =
          ((KademliaProtocol) Network.get(m).getProtocol(kademliaid)).getKademliaNode().getId();

      if (mId.equals(searchNodeId)) return Network.get(m);

      if (mId.compareTo(searchNodeId) < 0) inf = m + 1;
      else sup = m - 1;
    }

    // perform a traditional search for more reliability (maybe the network is not ordered)
    BigInteger mId;
    for (int i = Network.size() - 1; i >= 0; i--) {
      mId = ((KademliaProtocol) Network.get(i).getProtocol(kademliaid)).getKademliaNode().getId();
      if (mId.equals(searchNodeId)) return Network.get(i);
    }

    return null;
  }

  public Node getNode() {
    return nodeIdtoNode(this.getKademliaNode().getId());
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
    FindOperation fop = this.findOp.get(m.operationId);

    if (fop != null) {
      fop.elaborateResponse((BigInteger[]) m.body);

      logger.info("Handleresponse FindOperation " + fop.getId() + " " + fop.getAvailableRequests());
      // save received neighbour in the closest Set of fin operation

      BigInteger[] neighbours = (BigInteger[]) m.body;
      if (callback != null) callback.nodesFound(fop, neighbours);
      for (BigInteger neighbour : neighbours) routingTable.addNeighbour(neighbour);

      if (!fop.isFinished() && Arrays.asList(neighbours).contains(fop.getDestNode())) {
        logger.warning("Found node " + fop.getDestNode());
        if (callback != null) callback.operationComplete(fop);
        KademliaObserver.find_ok.add(1);
        fop.setFinished(true);
      }

      if (fop instanceof GetOperation && m.value != null && !fop.isFinished()) {
        fop.setFinished(true);
        callback.operationComplete(fop);
        ((GetOperation) fop).setValue(m.value);
        logger.warning(
            "Getprocess finished found " + ((GetOperation) fop).getValue() + " hops " + fop.nrHops);
      }

      while (fop.getAvailableRequests() > 0) { // I can send a new find request

        // get an available neighbour
        BigInteger neighbour = fop.getNeighbour();

        if (neighbour != null) {
          if (!fop.isFinished()) {
            // create a new request to send to neighbour
            Message request;
            if (fop instanceof GetOperation) request = new Message(Message.MSG_GET);
            else if (KademliaCommonConfig.FINDMODE == 0) request = new Message(Message.MSG_FIND);
            else request = new Message(Message.MSG_FIND_DIST);
            request.operationId = m.operationId;
            request.src = this.getKademliaNode();
            request.dst = nodeIdtoNode(neighbour).getKademliaProtocol().getKademliaNode();
            if (KademliaCommonConfig.FINDMODE == 0 || request.getType() == Message.MSG_GET)
              request.body = fop.getDestNode();
            else request.body = Util.logDistance(fop.getDestNode(), (BigInteger) fop.getBody());
            // increment hop count
            fop.nrHops++;

            // send find request
            sendMessage(request, neighbour, myPid);
            if (request.getType() == Message.MSG_FIND
                || request.getType() == Message.MSG_FIND_DIST) {}
          }
        } else if (fop.getAvailableRequests()
            == KademliaCommonConfig.ALPHA) { // no new neighbour and no outstanding requests
          // search operation finished

          if (fop instanceof PutOperation) {
            for (BigInteger id : fop.getNeighboursList()) {
              // create a put request
              Message request = new Message(Message.MSG_PUT);
              request.operationId = m.operationId;
              request.src = this.getKademliaNode();
              request.dst = nodeIdtoNode(id).getKademliaProtocol().getKademliaNode();
              request.body = ((PutOperation) fop).getBody();
              request.value = ((PutOperation) fop).getValue();
              // increment hop count
              fop.nrHops++;
              sendMessage(request, id, myPid);
            }
            logger.warning("Sending PUT_VALUE to " + fop.getNeighboursList().size() + " nodes");
          } else if (fop instanceof GetOperation) {
            findOp.remove(fop.getId());
            logger.warning("Getprocess finished not found ");

          } else {
            logger.warning("Find operation finished not found ");

            findOp.remove(fop.getId());
          }
          if (callback != null) callback.operationComplete(fop);

          if (fop.getBody().equals("Automatically Generated Traffic")
              && fop.getClosest().containsKey(fop.getDestNode())) {
            // update statistics
            long timeInterval = (CommonState.getTime()) - (fop.getTimestamp());
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
   * Response to a put request.<br>
   * Store the object in the key value store
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  private void handlePut(Message m) {
    logger.warning("Handle put sample:" + m.body);
    kv.add((BigInteger) m.body, m.value);
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

    logger.info("handleFind received from " + m.src.getId() + " " + m.operationId);
    BigInteger[] neighbours = new BigInteger[KademliaCommonConfig.K];
    if (m.getType() == Message.MSG_FIND || m.getType() == Message.MSG_GET) {
      neighbours = this.routingTable.getNeighbours((BigInteger) m.body, m.src.getId());
    } else if (m.getType() == Message.MSG_FIND_DIST) {
      neighbours = this.routingTable.getNeighbours((int) m.body);
    } else {
      return;
    }

    // for (BigInteger neigh : neighbours) logger.warning("Neighbours " + neigh);
    // create a response message containing the neighbours (with the same id of the request)
    Message response = new Message(Message.MSG_RESPONSE, neighbours);
    response.operationId = m.operationId;
    response.dst = m.dst;
    response.src = this.getKademliaNode();
    response.ackId = m.id; // set ACK number

    if (m.getType() == Message.MSG_GET) {
      response.value = kv.get((BigInteger) m.body);
    }
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
  public Operation handleInit(Message m, int myPid) {

    logger.info("handleInitFind " + (BigInteger) m.body);
    KademliaObserver.find_op.add(1);

    // create find operation and add to operations array
    // FindOperation fop = new FindOperation(m.dest, m.timestamp);

    FindOperation fop;

    switch (m.type) {
      case Message.MSG_INIT_FIND:
        fop = new FindOperation(this.node.getId(), (BigInteger) m.body, m.timestamp);
        break;
      case Message.MSG_INIT_GET:
        fop = new GetOperation(this.node.getId(), (BigInteger) m.body, m.timestamp);
        break;
      case Message.MSG_INIT_PUT:
        fop = new PutOperation(this.node.getId(), (BigInteger) m.body, m.timestamp);
        ((PutOperation) fop).setValue(m.value);
        break;
      default:
        fop = new FindOperation(this.node.getId(), (BigInteger) m.body, m.timestamp);
        break;
    }

    fop.setBody(m.body);
    findOp.put(fop.getId(), fop);

    // get the ALPHA closest node to srcNode and add to find operation
    BigInteger[] neighbours =
        this.routingTable.getNeighbours((BigInteger) m.body, this.getKademliaNode().getId());
    fop.elaborateResponse(neighbours);
    fop.setAvailableRequests(KademliaCommonConfig.ALPHA);

    // set message operation id
    m.operationId = fop.getId();

    m.src = this.getKademliaNode();

    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = fop.getNeighbour();
      if (nextNode != null) {
        m.dst =
            nodeIdtoNode(nextNode)
                .getKademliaProtocol()
                .getKademliaNode(); // new KademliaNode(nextNode);
        // set message type depending on find mode
        if (m.type == Message.MSG_INIT_GET) m.type = Message.MSG_GET;
        else if (KademliaCommonConfig.FINDMODE == 0) m.type = Message.MSG_FIND;
        else {
          m.type = Message.MSG_FIND_DIST;
          m.body = Util.logDistance(nextNode, (BigInteger) fop.getBody());
        }

        logger.info("sendMessage to " + nextNode);

        sendMessage(m.copy(), nextNode, myPid);
        if (m.getType() == Message.MSG_FIND_DIST) {
          fop.nrHops++;
        }
      }
    }
    return fop;
  }

  /**
   * send a message with current transport layer and starting the timeout timer (wich is an event)
   * if the message is a request
   *
   * @param m the message to send
   * @param destId the Id of the destination node
   * @param myPid the sender Pid
   */
  private void sendMessage(Message m, BigInteger destId, int myPid) {
    // add destination to routing table
    this.routingTable.addNeighbour(destId);
    // int destpid;
    assert m.src != null;
    assert m.dst != null;

    Node src = nodeIdtoNode(this.getKademliaNode().getId());
    Node dest = nodeIdtoNode(destId);

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
    if (event instanceof Message) {
      m = (Message) event;
      KademliaObserver.reportMsg(m, false);
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_RESPONSE:
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleResponse(m, myPid);
        break;

      case Message.MSG_INIT_FIND:
        m = (Message) event;
        handleInit(m, myPid);
        break;
      case Message.MSG_INIT_GET:
        m = (Message) event;
        handleInit(m, myPid);
        break;

      case Message.MSG_INIT_PUT:
        m = (Message) event;
        handleInit(m, myPid);
        break;

      case Message.MSG_FIND:
        m = (Message) event;
        handleFind(m, myPid);
        break;
      case Message.MSG_FIND_DIST:
        m = (Message) event;
        handleFind(m, myPid);
        break;
      case Message.MSG_GET:
        m = (Message) event;
        handleFind(m, myPid);
        break;

      case Message.MSG_PUT:
        m = (Message) event;
        handlePut(m);
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
    if (event instanceof Message) {
      OpLogging fLog;

      m = (Message) event;
      if (this.findLog.get(m.operationId) == null) {
        fLog = new OpLogging(m.operationId, this.node.getId(), CommonState.getTime(), m.getType());
        findLog.put(m.operationId, fLog);
      } else {
        fLog = this.findLog.get(m.operationId);
      }
      /*Operation Logging */
      fLog.AddMessage(m.id);
      fLog.SetStop(CommonState.getTime());
      findLog.put(m.operationId, fLog);
    }

    for (Map.Entry<Long, OpLogging> entry : findLog.entrySet()) {
      KademliaObserver.reportFindOp(entry.getValue());
    }
  }

  /** get the current Node */
  public KademliaNode getKademliaNode() {
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

  public void setEventsCallback(KademliaEvents callback) {
    this.callback = callback;
  }
}
