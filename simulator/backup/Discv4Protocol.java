package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import peersim.config.Configuration;
import peersim.core.Cleanable;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.kademlia.operations.LookupOperation;
import peersim.kademlia.operations.Operation;

public class Discv4Protocol extends KademliaProtocol implements Cleanable {

  // public TopicTable topicTable;

  /** Table to keep track of topic registrations */
  protected HashSet<String> activeTopics;

  private HashMap<Long, Long> registrationMap;

  final String PAR_DISC4_STOP = "DISCV4_STOP";

  public Discv4Protocol(String prefix) {

    super(prefix);

    activeTopics = new HashSet<String>();
    this.registrationMap = new HashMap<>();

    KademliaCommonConfig.DISCV4_STOP =
        Configuration.getInt(prefix + "." + PAR_DISC4_STOP, KademliaCommonConfig.DISCV4_STOP);
    // TODO Auto-generated constructor stub
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv4Protocol dolly = new Discv4Protocol(Discv4Protocol.prefix);
    return dolly;
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  protected void _init() {
    // execute once

    if (_ALREADY_INSTALLED) return;

    super._init();
  }

  public List<String> getRegisteringTopics() {

    return new ArrayList<String>(activeTopics);
  }

  /**
   * Start a register opearation.<br>
   * Find the ALPHA closest node and send register request to them.
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitRegister(Message m, int myPid) {

    Topic t = (Topic) m.body;
    // logger.warning("Sending topic registration for topic "+t.getTopic());

    activeTopics.add(t.getTopic());

    logger.warning("handleInitRegisterTopic " + t.getTopic() + " " + t.getTopicID());

    KademliaObserver.addTopicRegistration(t, this.node.getId());
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {
    // existing active destination node

    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger rand = urg.generate();

    Message m = Message.makeInitFindNode(rand);
    m.timestamp = CommonState.getTime();

    return m;
  }

  /**
   * Perform the required operation upon receiving a message in response to a ROUTE message.<br>
   * Update the find operation record with the closest set of neighbour received. Then, send as many
   * ROUTE request I can (according to the ALPHA parameter).<br>
   * If no closest neighbour available and no outstanding messages stop the find operation.
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  protected void handleResponse(Message m, int myPid) {
    Operation op = (Operation) this.operations.get(m.operationId);
    if (op == null) {
      return;
    }
    // add message source to my routing table
    BigInteger[] neighbours = (BigInteger[]) m.body;
    op.elaborateResponse(neighbours);
    for (BigInteger neighbour : neighbours) routingTable.addNeighbour(neighbour);

    int discovered = 0;
    if (registrationMap.get(op.operationId) != null) {
      LookupOperation lop = (LookupOperation) operations.get(registrationMap.get(op.operationId));
      lop.increaseReturned(m.src.getId());
      String topicString = lop.getTopic().getTopic();
      lop.addAskedNode(m.src.getId());

      if (m.src.is_evil) lop.increaseMaliciousQueries();

      if (!lop.finished) lop.increaseUsed(m.src.getId());

      int numEvilReturned = 0;
      int numPeersNotAskedBefore = 0;
      for (BigInteger returnedPeerId : neighbours) {
        // peers shouldn't return me in their response
        assert (!returnedPeerId.equals(this.node.getId()));
        // if(returnedPeerId.equals(this.node.getId()))continue;
        KademliaNode returnedPeer =
            Util.nodeIdtoNode(returnedPeerId).getKademliaProtocol().getNode();

        // don't do anything if we already checked the node
        if (!lop.nodeAlreadyAsked(returnedPeerId)) {
          numPeersNotAskedBefore++;
          // Count number of evil peers in the response if returned by honest node
          if (!m.src.is_evil && returnedPeer.is_evil) numEvilReturned++;
          // simulate sending a lookup
          Message request = new Message(Message.MSG_TOPIC_QUERY);
          request.operationId = lop.operationId;
          request.src = this.node;
          request.body = lop.body;
          request.dest = returnedPeer;
          KademliaObserver.reportMsg(request, false);

          // simulate receiving a response
          Message reply = new Message(Message.MSG_TOPIC_QUERY_REPLY);
          reply.src = returnedPeer;
          reply.body = lop.body;
          reply.dest = this.node;
          KademliaObserver.reportMsg(reply, false);

          if (returnedPeer.hasTopic(topicString)) {
            lop.addDiscovered(returnedPeer);
            KademliaObserver.addDiscovered(lop.topic, m.src.getId(), returnedPeerId);
          }

          // make sure we don't ask the same node again
          lop.addAskedNode(returnedPeerId);
        }
        discovered = lop.discoveredCount();

        if ((!op.finished && (Arrays.asList(neighbours).contains(op.destNode))
            ||
            // end if we discovered enough peers
            ((KademliaCommonConfig.DISCV4_STOP == 1
                && discovered >= KademliaCommonConfig.TOPIC_PEER_LIMIT)))) {
          logger.warning("Discv4 lookup node " + op.destNode);
          logger.warning("Found node " + op.destNode);
          op.finished = true;

          KademliaObserver.find_ok.add(1);

          if (registrationMap.get(op.operationId) != null) {
            KademliaObserver.reportOperation(lop);
            registrationMap.remove(op.operationId);
          }
          return;
        }
      }
      if (numEvilReturned > 0 && (numPeersNotAskedBefore == numEvilReturned))
        lop.increaseMalRespFromHonest();
    }

    op.increaseReturned(m.src.getId());
    if (!op.finished) op.increaseUsed(m.src.getId());

    while ((op.available_requests > 0)) { // I can send a new find request
      BigInteger neighbour = op.getNeighbour();

      if (neighbour != null) {
        if (!op.finished) {
          // send a new request only if we didn't find the node already
          Message request = null;
          if (op.type == Message.MSG_FIND) {
            request = new Message(Message.MSG_FIND);
            request.body = Util.logDistance(op.destNode, neighbour);
          } else if (op.type == Message.MSG_REGISTER) {
            request = new Message(Message.MSG_REGISTER);
            request.body = op.body;
          } else if (op.type == Message.MSG_TICKET_REQUEST) {
            request = new Message(Message.MSG_TICKET_REQUEST);
            request.body = op.body;
          }

          if (request != null) {

            if (registrationMap.get(op.operationId) != null) {
              LookupOperation lop =
                  (LookupOperation) operations.get(registrationMap.get(op.operationId));
              lop.nrHops++;
            }
            op.nrHops++;
            request.operationId = m.operationId;
            request.src = this.node;
            request.dest =
                Util.nodeIdtoNode(neighbour)
                    .getKademliaProtocol()
                    .getNode(); // new KademliaNode(neighbour);
            sendMessage(request, neighbour, myPid);
          }
        }

      } else if (op.available_requests
          == KademliaCommonConfig.ALPHA) { // no new neighbour and no outstanding requests
        operations.remove(op.operationId);

        logger.warning("Finished lookup node " + op.getUsedCount());

        if (registrationMap.get(op.operationId) != null && op.finished != true) {

          LookupOperation lop =
              (LookupOperation) operations.get(registrationMap.get(op.operationId));

          KademliaObserver.reportOperation(lop);

          registrationMap.remove(op.operationId);
          // FIXME removed
          // node.setLookupResult(lop.getDiscovered(),lop.getTopic().getTopic());
          // logger.warning("Handle response topic "+lop.getTopic().getTopic());
        }

        // KademliaObserver.reportOperation(op);
        if (!op.finished && op.type == Message.MSG_FIND) {
          logger.warning("Couldn't find node " + op.destNode);
        }
        return;

      } else { // no neighbour available but exists outstanding request to wait for
        return;
      }
    }
  }

  private void handleInitTopicLookup(Message m, int myPid) {

    KademliaObserver.lookup_total.add(1);
    Topic t = (Topic) m.body;

    logger.warning("discv4 Send init lookup for topic " + this.node.getId() + " " + t.getTopic());

    LookupOperation lop = new LookupOperation(this.node.getId(), m.timestamp, t);
    lop.body = m.body;
    lop.type = Message.MSG_TOPIC_QUERY;
    lop.nrHops += 3;
    operations.put(lop.operationId, lop);

    // generate a FIND message towards a random ID
    Message mFind = generateFindNodeMessage();
    long op = handleInitFind(mFind, myPid);

    registrationMap.put(op, lop.operationId);

    // logger.warning("Lookup operation id "+lop.operationId+" "+op);

  }

  /**
   * set the current NodeId
   *
   * @param tmp BigInteger
   */
  public void setNode(KademliaNode node) {
    super.setNode(node);
  }

  public void onKill() {
    // System.out.println("Node removed");
    // topicTable = null;
  }

  /*private void handleTimeout(Timeout t, int myPid){
  	logger.warning("Handletimeout");

  }*/

  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  public void processEvent(Node myNode, int myPid, Object event) {

    // logger.warning("Discv4 process event");
    super.processEvent(myNode, myPid, event);
    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_TOPIC_LOOKUP:
        m = (Message) event;
        handleInitTopicLookup(m, myPid);
        break;

      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegister(m, myPid);
        break;

        /*case Timeout.REG_TIMEOUT:

        String topic = ((Timeout) event).topic.getTopic();
        Message message= generateRegisterMessage(topic);
        logger.warning("Timeout "+topic);
           EDSimulator.add(0, message, Util.nodeIdtoNode(this.node.getId()),myPid);

        break;*/

      case Timeout.TIMEOUT: // timeout
        Timeout timeout = (Timeout) event;
        if (sentMsg.containsKey(timeout.msgID)) { // the response msg didn't arrived
          logger.info(
              "Node "
                  + this.node.getId()
                  + " received a timeout: "
                  + timeout.msgID
                  + " from: "
                  + timeout.node);
          // remove form sentMsg
          sentMsg.remove(timeout.msgID);
        }
        break;
    }
  }
}
