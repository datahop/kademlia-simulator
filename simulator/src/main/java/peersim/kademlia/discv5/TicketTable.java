package peersim.kademlia.discv5;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
// import java.util.Random;
import java.util.logging.Logger;
import peersim.kademlia.Message;
import peersim.kademlia.RoutingTable;
import peersim.kademlia.Util;

public class TicketTable extends RoutingTable {

  /** Table to keep track of topic registrations */
  private List<BigInteger> pendingTickets;

  private Discv5Protocol protocol;

  private Topic t;

  private int myPid;

  Logger logger;

  boolean refresh;

  HashMap<Integer, Integer> registeredPerDist;

  private List<BigInteger> registeredNodes;

  private int[] activeRegistrations;

  private List<Integer> seenOccupancy;

  public TicketTable(
      int nBuckets,
      int k,
      int maxReplacements,
      Discv5Protocol protocol,
      Topic t,
      int myPid,
      boolean refresh) {

    super(nBuckets, k, maxReplacements);

    pendingTickets = new ArrayList<BigInteger>();

    this.protocol = protocol;

    this.t = t;

    this.nodeId = t.getTopicID();

    this.myPid = myPid;

    logger = Logger.getLogger(protocol.getKademliaProtocol().getNode().getId().toString());
    // LogManager.getLogManager().reset();

    this.refresh = refresh;

    registeredPerDist = new HashMap<Integer, Integer>();

    registeredNodes = new ArrayList<BigInteger>();

    activeRegistrations = new int[nBuckets];

    seenOccupancy = new ArrayList<Integer>();
  }

  public boolean addNeighbour(BigInteger node) {

    if (!pendingTickets.contains(node) && !registeredNodes.contains(node)) {
      if (super.addNeighbour(node)) {
        pendingTickets.add(node);
        protocol.sendTicketRequest(node, t, myPid);
        addRegisteredList(node);

        return true;
      }
    }
    return false;
  }
  // add a neighbour to the correct k-bucket
  public void addNeighbour(BigInteger[] nodes) {
    for (BigInteger node : nodes) {
      addNeighbour(node);
    }
  }

  public void addTicket(Message m, Ticket ticket) {

    if (pendingTickets.contains(m.src.getId())) {

      Message register = new Message(Message.MSG_REGISTER, t);
      register.ackId = m.id;
      register.dest = m.src; // new KademliaNode(m.src);
      register.body = ticket;
      register.operationId = m.operationId;
      protocol.scheduleSendMessage(register, m.src.getId(), myPid, ticket.getWaitTime());

      int dist = Util.distance(nodeId, m.src.getId());
      if (!registeredPerDist.containsKey(dist)) {
        registeredPerDist.put(dist, 1);
      } else {
        registeredPerDist.put(dist, registeredPerDist.get(dist) + 1);
      }
    }
  }

  public void addTicketRandomTopic(Message m, Ticket ticket, Topic randomTopic) {

    if (pendingTickets.contains(m.src.getId())) {

      Message register = new Message(Message.MSG_REGISTER, randomTopic);
      register.ackId = m.id;
      register.dest = m.src; // new KademliaNode(m.src);
      register.body = ticket;
      register.operationId = m.operationId;
      protocol.scheduleSendMessage(register, m.src.getId(), myPid, ticket.getWaitTime());

      int dist = Util.distance(nodeId, m.src.getId());
      if (!registeredPerDist.containsKey(dist)) {
        registeredPerDist.put(dist, 1);
      } else {
        registeredPerDist.put(dist, registeredPerDist.get(dist) + 1);
      }
    }
  }

  // remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {
    // get the lenght of the longest common prefix (correspond to the correct k-bucket)
    pendingTickets.remove(node);
    getBucket(node).removeNeighbour(node);

    // int i = Util.logDistance(nodeId, node) - bucketMinDistance - 1;
    // BigInteger randomNode = generateRandomNode(i);
    // protocol.refreshBucket(this, randomNode,i);
    protocol.refreshBucket(this, Util.distance(nodeId, node));
  }

  public void addRegisteredList(BigInteger node) {
    registeredNodes.add(node);
  }

  public void removeRegisteredList(BigInteger node) {
    registeredNodes.remove(node);
  }

  public BigInteger getTopicId() {
    return nodeId;
  }

  public void acceptedReg(BigInteger node) {

    int dist = Util.distance(nodeId, node);
    int bucket;
    if (dist <= bucketMinDistance) bucket = 0;
    else bucket = dist - bucketMinDistance - 1;
    activeRegistrations[bucket]++;
  }

  public void expiredReg(BigInteger node) {

    int dist = Util.distance(nodeId, node);
    int bucket;
    if (dist <= bucketMinDistance) bucket = 0;
    else bucket = dist - bucketMinDistance - 1;
    if (activeRegistrations[bucket] > 0) activeRegistrations[bucket]--;
  }

  public void reportResponse(Ticket ticket) {
    seenOccupancy.add(ticket.getOccupancy());
  }

  public Topic getTopic() {
    return t;
  }
}
