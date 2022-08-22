package peersim.kademlia;

import java.math.BigInteger;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

public class Discv4EvilProtocol extends Discv4Protocol {

  // public TopicTable topicTable;
  private RoutingTable evilRoutingTable; // routing table only containing evil neighbors

  public Discv4EvilProtocol(String prefix) {

    super(prefix);
    this.evilRoutingTable =
        new RoutingTable(
            KademliaCommonConfig.NBUCKETS,
            KademliaCommonConfig.K,
            KademliaCommonConfig.MAXREPLACEMENT);
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv4EvilProtocol dolly = new Discv4EvilProtocol(Discv4EvilProtocol.prefix);
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

  /**
   * Start a register topic operation.<br>
   * If this is an on-going register operation with a previously obtained ticket, then send a
   * REGTOPIC message; otherwise, Find the ALPHA closest node and send REGTOPIC message to them
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitRegister(Message m, int myPid) {

    // logger.warning("Discv4 evil handleInitRegister");

    // Fill the evilRoutingTable only with other malicious nodes
    this.evilRoutingTable.setNodeId(this.node.getId());
    for (int i = 0; i < Network.size(); i++) {
      Node n = Network.get(i);
      KademliaProtocol prot = (KademliaProtocol) n.getKademliaProtocol();
      if (this.getNode().equals(prot.getNode())) continue;
      if (prot.getNode().is_evil) {
        this.evilRoutingTable.addNeighbour(prot.getNode().getId());
      }
    }
    // spamNodes(generateFindNodeMessage(),myPid);
    super.handleInitRegister(m, myPid);
  }

  protected void spamNodes(Message m, int pid) {

    // set message operation id
    m.operationId = 1;
    m.type = Message.MSG_FIND;
    m.src = this.node;
    for (int i = 0; i < KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = this.routingTable.getNeighbours(i);
      for (BigInteger id : neighbours) {
        m.body = i;
        m.dest = Util.nodeIdtoNode(id).getKademliaProtocol().getNode();
        sendMessage(m.copy(), id, pid);
      }
    }
  }

  /*protected void handleResponse(Message m, int myPid) {
  	//Do nothing
  }*/

  /**
   * Response to a route request.<br>
   * Respond with the peers in your k-bucket closest to the key that is being looked for
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  protected void handleFind(Message m, int myPid, int dist) {
    // get the ALPHA closest node to destNode

    // BigInteger[] neighbours = this.evilRoutingTable.getNeighbours(dist);
    BigInteger[] neighbours = new BigInteger[0];
    /*while (neighbours.length < 16 && dist < 256) {
      neighbours = this.evilRoutingTable.getNeighbours(dist);
      dist++;
    }

    List<BigInteger> tempList = new ArrayList<BigInteger>();

    while (tempList.size() < 16 && dist < 256) {
      neighbours = this.evilRoutingTable.getNeighbours(dist);
      tempList.addAll(Arrays.asList(neighbours));
      dist++;
    }

    // remove the source of message m from the results
    // List<BigInteger> tempList = new ArrayList<BigInteger>(Arrays.asList(neighbours));
    tempList.remove(m.src.getId());
    neighbours = tempList.toArray(new BigInteger[0]);*/
    // create a response message containing the neighbours (with the same id of the request)
    Message response = new Message(Message.MSG_RESPONSE, neighbours);
    response.operationId = m.operationId;
    // response.body = m.body;
    response.src = this.node;
    response.dest = m.src;
    response.ackId = m.id; // set ACK number

    // send back the neighbours to the source of the message
    sendMessage(response, m.src.getId(), myPid);
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

    logger.info("Discv4 evil process event");
    super.processEvent(myNode, myPid, event);
    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegister(m, myPid);
        break;
    }
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  protected Message generateFindNodeMessage() {
    // existing active destination node

    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger rand = urg.generate();

    Message m = Message.makeInitFindNode(rand);
    m.timestamp = CommonState.getTime();

    return m;
  }
}
