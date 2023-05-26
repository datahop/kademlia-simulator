package peersim.kademlia;

/**
 * A Kademlia implementation for PeerSim extending the EDProtocol class. See the Kademlia
 * bibliografy for more information about the protocol.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
import java.math.BigInteger;
import java.util.Arrays;
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
import peersim.kademlia.operations.FindOperation;
import peersim.kademlia.operations.GetOperation;
import peersim.kademlia.operations.OpLogging;
import peersim.kademlia.operations.Operation;
import peersim.kademlia.operations.PutOperation;
import peersim.kademlia.operations.RegionBasedFindOperation;
import peersim.transport.UnreliableTransport;

/**
 * KademliaProtocol is a class that builds ontop of the EDProtocol interface to implement the
 * Kademlia protocol.
 *
 * @see Cloneable
 * @see EDProtocol
 */
public class KademliaProtocol implements Cloneable, EDProtocol {

  // VARIABLE PARAMETERS
  /** The parameter name for K. */
  final String PAR_K = "K";
  /** The parameter name for ALPHA. */
  final String PAR_ALPHA = "ALPHA";
  /** The parameter name for BITS. */
  final String PAR_BITS = "BITS";
  /** The parameter name for FINDMODE. */
  final String PAR_FINDMODE = "FINDMODE";

  /** The parameter name for transport. */
  private static final String PAR_TRANSPORT = "transport";

  /** Prefix for configuration parameters. */
  private static String prefix = null;

  /** UnreliableTransport object used for communication. */
  private UnreliableTransport transport;

  /** Identifier for the tranport protocol (used in teh sendMessage method) - TODO: verify!!!. */
  private int tid;

  /** Unique ID for this Kademlia node/network - TODO: verify!!!. */
  private int kademliaid;

  /** Indicates if the service initializer has already been called. */
  private static boolean _ALREADY_INSTALLED = false;

  /** Routing table of this Pastry node. */
  private RoutingTable routingTable;

  /** TreeMap containing trace messages sent for timeout purposes. */
  private TreeMap<Long, Long> sentMsg;

  /** LinkedHashMap containing find operations. */
  private LinkedHashMap<Long, FindOperation> findOp;

  /** Kademlia node instance. */
  public KademliaNode node;

  /** Logging handler. */
  protected Logger logger;

  /** Key-value store. */
  private KeyValueStore kv;

  /** Callback for Kademlia events. */
  private KademliaEvents callback;

  /** LinkedHashMap containing logging information for find operations. */
  public LinkedHashMap<Long, OpLogging> findLog;

  /**
   * Replicate this object by returning an identical copy. It is called by the initializer and do
   * not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    KademliaProtocol dolly = new KademliaProtocol(KademliaProtocol.prefix);
    return dolly;
  }

  /**
   * Constructor for KademliaProtocol. It is only used by the initializer when creating the
   * prototype. Every other instance calls CLONE to create a new object.
   *
   * @param prefix String: the prefix for configuration parameters
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

    kv = new KeyValueStore();

    // System.out.println("New kademliaprotocol");
  }

  /**
   * This procedure is called only once and allows to initialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  private void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    // read parameters
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
   * Search through the network for a node with a specific node ID, using binary search based on the
   * ordering of the network. If the binary search does not find a node with the given ID, a
   * traditional search is performed for more reliability (in case the network is not ordered).
   *
   * @param searchNodeId the ID of the node to search for
   * @return the node with the given ID, or null if not found
   */
  public Node nodeIdtoNode(BigInteger searchNodeId) {
    // If the given searchNodeId is null, return null
    if (searchNodeId == null) return null;

    // Set the initial search range to cover the entire network
    int inf = 0;
    int sup = Network.size() - 1;
    int m;

    // Perform binary search until the search range is empty
    while (inf <= sup) {
      // Calculate the midpoint of the search range
      m = (inf + sup) / 2;

      // Get the ID of the node at the midpoint
      BigInteger mId =
          ((KademliaProtocol) Network.get(m).getProtocol(kademliaid)).getKademliaNode().getId();

      // If the midpoint node has the desired ID, return it
      if (mId.equals(searchNodeId)) return Network.get(m);

      // If the midpoint node has a smaller ID than the desired ID, narrow the search range to the
      // upper half of the current range
      if (mId.compareTo(searchNodeId) < 0) inf = m + 1;
      // Otherwise, narrow the search range to the lower half of the current range
      else sup = m - 1;
    }

    // If the binary search did not find a node with the desired ID, perform a traditional search
    // through the network
    BigInteger mId;
    for (int i = Network.size() - 1; i >= 0; i--) {
      mId = ((KademliaProtocol) Network.get(i).getProtocol(kademliaid)).getKademliaNode().getId();
      if (mId.equals(searchNodeId)) return Network.get(i);
    }

    // If no node with the desired ID was found, return null
    return null;
  }

  /**
   * Gets the node associated with this Kademlia protocol instance by calling nodeIdtoNode method
   * with the ID of this KademliaNode. TODO: verify.
   *
   * @return the node associated with this Kademlia protocol instance,
   */
  public Node getNode() {
    return nodeIdtoNode(this.getKademliaNode().getId());
  }

  /**
   * Perform the required operation upon receiving a message in response to a ROUTE (FIND would be
   * more appropriate here) message. Update the find operation record with the closest set of
   * neighbors received. Then, send as many ROUTE requests as possible (according to the ALPHA
   * parameter). If there are no closest neighbors available and no outstanding messages, stop the
   * find operation.
   *
   * @param m the message received.
   * @param myPid the sender PID.
   */
  private void handleResponse(Message m, int myPid) {
    // Add the message source to my routing table
    if (m.src != null) {
      routingTable.addNeighbour(m.src.getId());
    }

    // Get the corresponding find operation record
    FindOperation fop = this.findOp.get(m.operationId);
    if (fop != null) {
      // Update the find operation record with the closest set of neighbors received
      fop.elaborateResponse((BigInteger[]) m.body);
      fop.AddMessage(m.id);
      logger.info("Handleresponse FindOperation " + fop.getId() + " " + fop.getAvailableRequests());
      // save received neighbour in the closest Set of fin operation
      BigInteger[] neighbours = (BigInteger[]) m.body;
      if (callback != null) callback.nodesFound(fop, neighbours);
      for (BigInteger neighbour : neighbours) routingTable.addNeighbour(neighbour);

      if (!fop.isFinished()
          && Arrays.asList(neighbours).contains(fop.getDestNode())
          && !(fop instanceof RegionBasedFindOperation)) {

        logger.warning("Found node " + fop.getDestNode());

        // Complete the operation and log the result.
        if (callback != null) {
          callback.operationComplete(fop);
        }
        KademliaObserver.find_ok.add(1);
        fop.setFinished(true);
        // KademliaObserver.reportOperation(fop);
        // findOp.remove(fop.getId());
      }

      // Check if it's a GetOperation and the value has been found
      if (fop instanceof GetOperation && m.value != null && !fop.isFinished()) {
        fop.setFinished(true);

        // Complete the operation and log the result
        if (callback != null) {
          callback.operationComplete(fop);
        }
        ((GetOperation) fop).setValue(m.value);
        logger.info(
            "Getprocess finished found " + ((GetOperation) fop).getValue() + " hops " + fop.nrHops);
        // KademliaObserver.reportOperation(fop);
        // findOp.remove(fop.getId());
      }

      // Send as many ROUTE requests as possible (according to the ALPHA parameter)
      while (fop.getAvailableRequests() > 0 && !fop.isFinished()) {
        // Get an available neighbour
        BigInteger neighbour = fop.getNeighbour();

        if (neighbour != null) {
          // Create a new request to send to neighbour
          Message request;

          if (fop instanceof GetOperation) {
            request = new Message(Message.MSG_GET);
          } else if (KademliaCommonConfig.FINDMODE == 0) {
            request = new Message(Message.MSG_FIND);
          } else {
            request = new Message(Message.MSG_FIND_DIST);
          }

          request.operationId = m.operationId;
          request.src = this.getKademliaNode();
          request.dst = nodeIdtoNode(neighbour).getKademliaProtocol().getKademliaNode();

          if (KademliaCommonConfig.FINDMODE == 0 || request.getType() == Message.MSG_GET) {
            request.body = fop.getDestNode();
          } else {
            request.body = Util.logDistance(fop.getDestNode(), (BigInteger) fop.getBody());
          }

          // Increment hop count
          fop.nrHops++;
          // Add message to operation Todo: verify
          fop.AddMessage(m.id);
          // Send find request to neighbor
          sendMessage(request, neighbour, myPid);

        } else if (fop.getAvailableRequests()
            == KademliaCommonConfig.ALPHA) { // No new neighbor and no outstanding requests
          // Search operation finished
          if (fop instanceof PutOperation) {
            // Create and send a put request to all neighbors in the neighbors list
            for (BigInteger id : fop.getNeighboursList()) {
              // Create a put request
              Message request = new Message(Message.MSG_PUT);
              request.operationId = m.operationId;
              request.src = this.getKademliaNode();
              request.dst = nodeIdtoNode(id).getKademliaProtocol().getKademliaNode();
              request.body = ((PutOperation) fop).getBody();
              request.value = ((PutOperation) fop).getValue();

              // Increment hop count
              fop.nrHops++;
              // Add message to operation
              fop.AddMessage(m.id);
              // Todo: verify
              sendMessage(request, id, myPid);
            }
            logger.info("Sending PUT_VALUE to " + fop.getNeighboursList().size() + " nodes");
          } else if (fop instanceof GetOperation) {
            // Remove the find operation record
            findOp.remove(fop.getId());
            logger.info("Getprocess finished not found ");
            KademliaObserver.reportOperation(fop);
          } else if (fop instanceof RegionBasedFindOperation) {
            findOp.remove(fop.getId());
            logger.info("Region-based lookup completed ");
            KademliaObserver.reportOperation(fop);

            for (BigInteger id : fop.getNeighboursList()) {
              logger.info("Found node " + id);
            }
          } else {
            logger.warning("Find operation finished not found ");
            KademliaObserver.reportOperation(fop);
            findOp.remove(fop.getId());
            KademliaObserver.reportOperation(fop);
          }

          if (callback != null) {
            callback.operationComplete(fop);
          }
          if (callback != null) callback.operationComplete(fop);

          if (fop.getBody().equals("Automatically Generated Traffic")
              && fop.getClosest().containsKey(fop.getDestNode())) {
            // Update statistics
            long timeInterval = (CommonState.getTime()) - (fop.getTimestamp());
            KademliaObserver.timeStore.add(timeInterval);
            KademliaObserver.hopStore.add(fop.nrHops);
            KademliaObserver.msg_deliv.add(1);
          }

          return;

        } else { // no neighbour available but exists oustanding request to wait
          logger.info(" no neighbour available but exists oustanding request to wait");
          return;
        }
      }
      if (fop.isFinished() && fop.getAvailableRequests() == KademliaCommonConfig.ALPHA) {
        logger.warning("Operation completed. reporting...");
        KademliaObserver.reportOperation(fop);
        findOp.remove(fop.getId());
        // Update statistics
        // long timeInterval = (CommonState.getTime()) - (fop.getTimestamp());
        // KademliaObserver.timeStore.add(timeInterval);
        // KademliaObserver.hopStore.add(fop.nrHops);
        // KademliaObserver.msg_deliv.add(1);
      }
    }
  }

  /**
   * Handles a put request received by the node Store the object in the key value store associated
   * with teh node
   *
   * @param m The message containing the put request.
   */
  private void handlePut(Message m) {
    logger.info("Handle put sample:" + m.body);
    kv.add((BigInteger) m.body, m.value);
  }

  /**
   * Handles the response to a route request by finding the ALPHA closest node consulting the
   * k-buckets and returning them to the sender.
   *
   * @param m Message object containing the request
   * @param myPid the ID of the sender node
   */
  private void handleFind(Message m, int myPid) {
    // Retrieve the ALPHA closest node to the destination node
    logger.info(
        "Received handleFind request from node "
            + m.src.getId()
            + " for operation "
            + m.operationId);
    BigInteger[] neighbours = new BigInteger[KademliaCommonConfig.K];
    // Determine which neighbors to retrieve based on the type of message
    if (m.getType() == Message.MSG_FIND || m.getType() == Message.MSG_GET) {
      // Retrieve the k nearest neighbors for the provided key
      neighbours = this.routingTable.getNeighbours((BigInteger) m.body, m.src.getId());
    } else if (m.getType() == Message.MSG_FIND_DIST) {
      // Retrieve the k neighbors within a distance range
      neighbours = this.routingTable.getNeighbours((int) m.body);
    } else {
      // Invalid message type
      return;
    }

    // for (BigInteger neigh : neighbours) logger.warning("Neighbours " + neigh);
    // Create a response message containing the retrieved neighbours
    Message response = new Message(Message.MSG_RESPONSE, neighbours);
    response.operationId = m.operationId;
    response.dst = m.dst;
    response.src = this.getKademliaNode();
    response.ackId = m.id; // set ACK number

    // Retrieve the value associated with the provided key (if applicable)
    if (m.getType() == Message.MSG_GET) {
      response.value = kv.get((BigInteger) m.body);
    }

    // Send the response message containing the neighbours (and optional value) back to the sender
    // node
    sendMessage(response, m.src.getId(), myPid);
  }

  /**
   * This method starts a find node operation, which searches for the ALPHA closest nodes to the
   * provided node ID and sends a find request to them.
   *
   * @param m Message object containing the node ID to find
   * @param myPid the ID of the sender node
   * @return a reference to the created operation object
   */
  public Operation handleInit(Message m, int myPid) {
    logger.info("handleInitFind " + (BigInteger) m.body);
    KademliaObserver.find_op.add(1);

    // Create find operation and add to operations array
    // FindOperation fop = new FindOperation(m.dest, m.timestamp);

    // Create find operation
    FindOperation fop;
    // Determine the type of the received message and create a corresponding operation object
    switch (m.type) {
      case Message.MSG_INIT_FIND_REGION_BASED:
        fop =
            new RegionBasedFindOperation(
                this.node.getId(), (BigInteger) m.body, (int) m.value, m.timestamp);
        break;
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

    // Set the body of the operation object to the node ID to find
    fop.setBody(m.body);
    // Add the operation object to the find operation hash map
    findOp.put(fop.getId(), fop);

    // Retrieve the ALPHA closest nodes to the source node and add them to the find operation
    BigInteger[] neighbours =
        this.routingTable.getNeighbours((BigInteger) m.body, this.getKademliaNode().getId());
    fop.elaborateResponse(neighbours);
    fop.setAvailableRequests(KademliaCommonConfig.ALPHA);

    // Set the operation ID of the message
    m.operationId = fop.getId();

    // Set the source of the message to the current node
    m.src = this.getKademliaNode();

    // Send ALPHA messages to the closest nodes
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = fop.getNeighbour();

      if (nextNode != null) {
        // Set the destination of the message to the next closest node
        m.dst =
            nodeIdtoNode(nextNode)
                .getKademliaProtocol()
                .getKademliaNode(); // new KademliaNode(nextNode);

        // Set the type of the message depending on the find mode
        if (m.type == Message.MSG_INIT_GET) m.type = Message.MSG_GET;
        else if (KademliaCommonConfig.FINDMODE == 0) m.type = Message.MSG_FIND;
        else {
          m.type = Message.MSG_FIND_DIST;
          m.body = Util.logDistance(nextNode, (BigInteger) fop.getBody());
        }

        // Send the message to the next closest node and add it to the operation's message list
        logger.info("sendMessage to " + nextNode);
        Message mbis = m.copy();
        fop.AddMessage(mbis.id);
        sendMessage(mbis, nextNode, myPid);
        // Increment the hop count of the operation if it's a distance-based find
        if (m.getType() == Message.MSG_FIND_DIST) {
          fop.nrHops++;
        }
      }
    }
    // Return a reference to the created operation object
    return fop;
  }

  /**
   * Sends a message using the current transport layer and starts the timeout timer if the message
   * is a request.
   *
   * @param m the message to send
   * @param destId the ID of the destination node
   * @param myPid the sender process ID (Todo: verify what myPid stand for!!!)
   */
  private void sendMessage(Message m, BigInteger destId, int myPid) {
    // Add destination node to routing table
    this.routingTable.addNeighbour(destId);
    // int destpid;

    // Assert that message source and destination nodes are not null
    assert m.src != null;
    assert m.dst != null;

    // Get source and destination nodes
    Node src = nodeIdtoNode(this.getKademliaNode().getId());
    Node dest = nodeIdtoNode(destId);

    // destpid = dest.getKademliaProtocol().getProtocolID();

    // Get the transport protocol
    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);

    // Send the message
    transport.send(src, dest, m, kademliaid);

    // If the message is a request, start the timeout timer
    if (m.getType() == Message.MSG_FIND || m.getType() == Message.MSG_FIND_DIST) {
      // Create a timeout object
      Timeout t = new Timeout(destId, m.id, m.operationId);

      // Get the latency of the network between the source and destination nodes
      long latency = transport.getLatency(src, dest);

      // Add the message to the sent messages map
      this.sentMsg.put(m.id, m.timestamp);

      // Schedule the timeout timer with a delay equal to 4 times the network latency
      EDSimulator.add(4 * latency, t, src, myPid); // set delay = 4*RTT
    }
  }

  /**
   * Handles the receiving of events by the peersim framework.
   *
   * @param myNode the current node receiving the event.
   * @param myPid the process ID of the current node. (TODO: verify!!!)
   * @param event the event being received by the current node.
   */
  public void processEvent(Node myNode, int myPid, Object event) {
    // Set the Kademlia ID as the current process ID - assuming Pid stands for process ID.
    this.kademliaid = myPid;

    Message m;

    // If the event is a message, report the message to the Kademlia observer.
    if (event instanceof Message) {
      m = (Message) event;
      KademliaObserver.reportMsg(m, false);
    }

    // Handle the event based on its type.
    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_RESPONSE:
        // Handle a response message by removing it from the sentMsg map and calling
        // handleResponse().
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleResponse(m, myPid);
        break;

      case Message.MSG_INIT_FIND_REGION_BASED:
      case Message.MSG_INIT_FIND:
      case Message.MSG_INIT_GET:
      case Message.MSG_INIT_PUT:
        // Handle an initialization message by calling handleInit().
        m = (Message) event;
        handleInit(m, myPid);
        break;

      case Message.MSG_FIND:
      case Message.MSG_FIND_DIST:
      case Message.MSG_GET:
        // Handle a find or get message by calling handleFind().
        m = (Message) event;
        handleFind(m, myPid);
        break;

      case Message.MSG_PUT:
        // Handle a put message by calling handlePut().
        m = (Message) event;
        handlePut(m);
        break;

      case Message.MSG_EMPTY:
        // TODO: Implement handling for an empty message.
        break;

      case Message.MSG_STORE:
        // TODO: Implement handling for a store message.
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
    /*if (event instanceof Message) {
    OpLogging fLog;

    m = (Message) event;
    if (this.findLog.get(m.operationId) == null) {
      fLog = new OpLogging(m.operationId, this.node.getId(), CommonState.getTime(), m.getType());
      findLog.put(m.operationId, fLog);
    } else {
      fLog = this.findLog.get(m.operationId);
    }
    /*Operation Logging */
    /*fLog.AddMessage(m.id);
      fLog.SetStop(CommonState.getTime());
      findLog.put(m.operationId, fLog);

      KademliaObserver.reportFindOp(fLog);
    }*/
    /*for (Map.Entry<Long, OpLogging> entry : findLog.entrySet()) {
      KademliaObserver.reportFindOp(entry.getValue());
    }*/
  }

  /**
   * Get the current KademliaNode object.
   *
   * @return The current KademliaNode object.
   */
  public KademliaNode getKademliaNode() {
    return this.node;
  }

  /**
   * Get the Kademlia node routing table.
   *
   * @return The Kademlia node routing table.
   */
  public RoutingTable getRoutingTable() {
    return this.routingTable;
  }

  /**
   * Set the protocol ID for this node.
   *
   * @param protocolID The protocol ID to set.
   */
  public void setProtocolID(int protocolID) {
    this.kademliaid = protocolID;
  }

  /**
   * Get the protocol ID for this node.
   *
   * @return The protocol ID for this node.
   */
  public int getProtocolID() {
    return this.kademliaid;
  }

  /**
   * Sets the current Kademlia node and its routing table.
   *
   * @param node The KademliaNode object to set.
   */
  public void setNode(KademliaNode node) {
    this.node = node;
    // Set the node ID in the routing table
    this.routingTable.setNodeId(node.getId());

    // Initialize the logger with the node ID as its name
    logger = Logger.getLogger(node.getId().toString());

    // Disable the logger's parent handlers to avoid duplicate output
    logger.setUseParentHandlers(false);

    // Set the logger's level to WARNING
    logger.setLevel(Level.WARNING);
    // logger.setLevel(Level.ALL);

    // Create a console handler for the logger
    ConsoleHandler handler = new ConsoleHandler();
    // Set the handler's formatter to a custom format that includes the time and logger name
    handler.setFormatter(
        new SimpleFormatter() {
          private static final String format = "[%d][%s] %3$s %n";

          @Override
          public synchronized String format(LogRecord lr) {
            return String.format(format, CommonState.getTime(), logger.getName(), lr.getMessage());
          }
        });
    // Add the console handler to the logger
    logger.addHandler(handler);
  }

  /**
   * Get the logger associated with this Kademlia node.
   *
   * @return The logger object.
   */
  public Logger getLogger() {
    return this.logger;
  }

  /**
   * Set the callback function for Kademlia events.
   *
   * @param callback The callback function to set.
   */
  public void setEventsCallback(KademliaEvents callback) {
    this.callback = callback;
  }
}
