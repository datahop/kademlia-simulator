package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
// import java.util.Random;
import java.util.logging.Logger;
import peersim.core.CommonState;
import peersim.core.Node;

public class TicketTable extends RoutingTable {

  /** Table to keep track of topic registrations */
  private List<BigInteger> pendingTickets;

  private Discv5TicketProtocol protocol;

  private Topic t;

  private int myPid;

  Logger logger;

  boolean refresh;

  HashMap<Integer, Integer> registeredPerDist;

  private List<BigInteger> registeredNodes;

  private int[] activeRegistrations;

  private int lastAskedBucket;
  private int triesWithinBucket;
  private List<Integer> seenOccupancy;
  private int seenNotFull = 0;

  private int available_requests;

  public TicketTable(
      int nBuckets,
      int k,
      int maxReplacements,
      Discv5TicketProtocol protocol,
      Topic t,
      int myPid,
      boolean refresh) {

    super(nBuckets, k, maxReplacements);

    pendingTickets = new ArrayList<BigInteger>();

    this.protocol = protocol;

    this.t = t;

    this.nodeId = t.getTopicID();

    this.myPid = myPid;

    logger = Logger.getLogger(protocol.getNode().getId().toString());
    // LogManager.getLogManager().reset();

    this.refresh = refresh;

    registeredPerDist = new HashMap<Integer, Integer>();

    registeredNodes = new ArrayList<BigInteger>();

    activeRegistrations = new int[nBuckets];
    lastAskedBucket = KademliaCommonConfig.BITS;

    seenOccupancy = new ArrayList<Integer>();

    available_requests = KademliaCommonConfig.ALPHA;
  }

  public boolean addNeighbour(BigInteger node) {
    // logger.info("Adding neighbour "+bucketWithRegs()+" "+KademliaCommonConfig.MAX_REG_BUCKETS+"
    // "+Util.logDistance(nodeId, node)+" "+(Util.logDistance(nodeId, node) - bucketMinDistance -
    // 1)+" "+(nBuckets-KademliaCommonConfig.MAX_REG_BUCKETS));
    /*if(bucketWithRegs()>=KademliaCommonConfig.MAX_REG_BUCKETS&&KademliaCommonConfig.MAX_REG_BUCKETS>0) {
    	int dist = Util.logDistance(nodeId, node);
    	int bucket;
    	if(dist<=bucketMinDistance)bucket = 0;
    	else bucket = dist - bucketMinDistance - 1;
    	if(bucket<(nBuckets-KademliaCommonConfig.MAX_REG_BUCKETS)) {
    		logger.info("Return false");
    		return false;
    	}
    }*/
    if (!pendingTickets.contains(node) && !registeredNodes.contains(node)) {
      if (super.addNeighbour(node)) {
        pendingTickets.add(node);
        // if(KademliaCommonConfig.PARALLELREGISTRATIONS==1)
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

  public BigInteger getNeighbour() {
    BigInteger res = null;

    /*if(!shallContinueRegistration()) {
    	System.out.println("Decided not to continue registration anymore");
    	this.available_requests = -KademliaCommonConfig.ALPHA;
    	return null;
    }*/

    while (lastAskedBucket > bucketMinDistance
        && triesWithinBucket >= super.bucketAtDistance(lastAskedBucket).occupancy()) {
      lastAskedBucket--;
      triesWithinBucket = 0;
    }
    if (lastAskedBucket > bucketMinDistance) {
      res = super.bucketAtDistance(lastAskedBucket).neighbours.get(triesWithinBucket);
      triesWithinBucket++;
    }
    // System.out.println("returning neighbour " + triesWithinBucket + " from bucket " +
    // lastAskedBucket);
    return res;
    // protocol.sendTicketRequest(node,t,myPid);
  }

  public void addTicket(Message m, Ticket ticket) {

    if (pendingTickets.contains(m.src.getId())) {

      Message register = new Message(Message.MSG_REGISTER, t);
      register.ackId = m.id;
      register.dest = m.src; // new KademliaNode(m.src);
      register.body = ticket;
      register.operationId = m.operationId;
      protocol.scheduleSendMessage(register, m.src.getId(), myPid, ticket.getWaitTime());

      int dist = Util.logDistance(nodeId, m.src.getId());
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

      int dist = Util.logDistance(nodeId, m.src.getId());
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
    protocol.refreshBucket(this, Util.logDistance(nodeId, node));
  }

  /** Check nodes and replace buckets with valid nodes from replacement list */
  public void refreshBuckets() {

    int i = CommonState.r.nextInt(nBuckets);

    KBucket b = k_buckets[i];

    List<BigInteger> toRemove = new ArrayList<BigInteger>();
    for (BigInteger n : b.neighbours) {
      Node node = Util.nodeIdtoNode(n);
      if (!node.isUp()) {
        toRemove.add(n);
      }
    }

    for (BigInteger n : toRemove) removeNeighbour(n);

    while (b.neighbours.size() < b.k && b.replacements.size() > 0) {

      BigInteger n = b.replacements.get(CommonState.r.nextInt(b.replacements.size()));

      Node node = Util.nodeIdtoNode(n);

      if (node.isUp()) addNeighbour(n);
      b.replacements.remove(n);
    }

    // BigInteger randomNode = null;

    if (b.replacements.size() == 0 || b.neighbours.size() < b.k) {
      // randomNode = generateRandomNode(i);
      // protocol.refreshBucket(this, randomNode,i);
      protocol.refreshBucket(this, i);
    }

    if (b.neighbours.size() == 0 && refresh) {
      protocol.sendLookup(nodeId, myPid);
    }
  }

  public void addRegisteredList(BigInteger node) {
    registeredNodes.add(node);
  }

  public void removeRegisteredList(BigInteger node) {
    registeredNodes.remove(node);
  }

  public void print() {
    logger.warning("Ticket table topic " + t.getTopic() + " :");
    int sum = 0;
    for (int dist = 256; dist > bucketMinDistance; dist--) {
      int removed = 0;
      if (registeredPerDist.containsKey(dist)) removed = registeredPerDist.get(dist);

      logger.warning(
          "Ticket "
              + t.getTopic()
              + " b["
              + dist
              + "]: "
              + super.bucketAtDistance(dist).occupancy()
              + " replacements:"
              + super.bucketAtDistance(dist).replacements.size()
              + " +"
              + removed);
      sum += removed;
    }
    logger.warning("Asked " + t.getTopic() + " " + sum + " nodes.");
  }

  public BigInteger getTopicId() {
    return nodeId;
  }

  public void acceptedReg(BigInteger node) {

    int dist = Util.logDistance(nodeId, node);
    int bucket;
    if (dist <= bucketMinDistance) bucket = 0;
    else bucket = dist - bucketMinDistance - 1;
    activeRegistrations[bucket]++;
  }

  public void expiredReg(BigInteger node) {

    int dist = Util.logDistance(nodeId, node);
    int bucket;
    if (dist <= bucketMinDistance) bucket = 0;
    else bucket = dist - bucketMinDistance - 1;
    if (activeRegistrations[bucket] > 0) activeRegistrations[bucket]--;
  }

  public int bucketWithRegs() {

    int regs = 0;
    for (int i : activeRegistrations) if (i > 0) regs++;

    return regs;
  }
  /* Not used anymore
  public boolean shallContinueRegistration() {
  	int windowSize = Math.min(seenOccupancy.size(), KademliaCommonConfig.STOP_REGISTER_WINDOW_SIZE);
  	System.out.println("STOP_REGISTER_WINDOW_SISZE: " + KademliaCommonConfig.STOP_REGISTER_WINDOW_SIZE + " STOP_REGISTER_MIN_REGS: " + KademliaCommonConfig.STOP_REGISTER_MIN_REGS + " windowsSize:" + windowSize);
  	if(seenOccupancy.size() < KademliaCommonConfig.STOP_REGISTER_MIN_REGS || windowSize == 0) {
  		System.out.println("Windows size 0 or seenOccupancy.size() < KademliaCommonConfig.STOP_REGISTER_MIN_REGS");
  		return true;
  	}

  	int sumOccupancy = 0;
  	int sumSpace = windowSize * KademliaCommonConfig.ADS_PER_QUEUE;
  	for(int i = (seenOccupancy.size() - windowSize); i < seenOccupancy.size(); i++) {
  	    sumOccupancy += seenOccupancy.get(i);
  	}


  	int toss = CommonState.r.nextInt(sumSpace);
  	System.out.println("Flipping coin. sumSpace: " + sumSpace + " seenOccupancy: " + seenOccupancy + " toss: " + toss);
  	if(toss < sumOccupancy) {
  		return false;
  	}
  	return true;
  }*/

  public void reportResponse(Ticket ticket) {
    seenOccupancy.add(ticket.getOccupancy());
  }

  public void increaseAvailableRequests() {
    available_requests++;
  }

  public void decreaseAvailableRequests() {
    available_requests--;
  }

  public int getAvailableRequests() {
    return available_requests;
  }

  public Topic getTopic() {
    return t;
  }
}
