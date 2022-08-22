package peersim.kademlia;

/** Discv5 Ticket Evil Protocol implementation. */
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.operations.RegisterOperation;

public class Discv5EvilDHTProtocol extends Discv5DHTProtocol {

  // VARIABLE PARAMETERS
  final String PAR_ATTACK_TYPE = "attackType";
  final String PAR_REFRESH_TIME = "refreshTime";

  UniformRandomGenerator urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
  // type of attack (TopicSpam)
  private String attackType;
  // number of registrations to make

  private boolean first = true;
  private HashMap<Topic, ArrayList<TopicRegistration>> evilTopicTable;
  private RoutingTable evilRoutingTable; // routing table only containing evil neighbors
  private HashMap<Topic, HashMap<BigInteger, Long>> initTicketRequestTime;
  private HashMap<Topic, HashMap<BigInteger, Long>> previousTicketRequestTime;

  private HashMap<String, List<BigInteger>> allSeen;

  private int refreshTime;

  private int n_regs;
  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5EvilDHTProtocol dolly = new Discv5EvilDHTProtocol(Discv5EvilDHTProtocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public Discv5EvilDHTProtocol(String prefix) {
    super(prefix);

    this.allSeen = new HashMap<>();
    this.attackType = Configuration.getString(prefix + "." + PAR_ATTACK_TYPE);
    this.refreshTime = Configuration.getInt(prefix + "." + PAR_REFRESH_TIME, 60000);

    this.n_regs = Configuration.getInt(prefix + "." + PAR_N, KademliaCommonConfig.N);
    this.evilTopicTable = new HashMap<Topic, ArrayList<TopicRegistration>>();
    this.evilRoutingTable =
        new RoutingTable(
            KademliaCommonConfig.NBUCKETS,
            KademliaCommonConfig.K,
            KademliaCommonConfig.MAXREPLACEMENT);
    // logger is not initialised at this point
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_HYBRID))
      System.out.println("Attacker type Hybrid");
    else if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_MALICIOUS_REGISTRAR))
      System.out.println("Attacker type Malicious Registrar");
    else if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM))
      System.out.println("Attacker type Topic Spam");
    else if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_DOS))
      System.out.println("Attacker type Dos");
    else if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)) {
      System.out.println("Attacker type waiting time spam");
      this.initTicketRequestTime = new HashMap<>();
      this.previousTicketRequestTime = new HashMap<>();

    } else {
      System.out.println("Invalid attacker type");
      System.exit(1);
    }
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
    Topic t = (Topic) m.body;

    logger.warning(
        "In handleInitRegister of EVIL "
            + t.getTopic()
            + " "
            + t.getTopicID()
            + " "
            + n_regs
            + " "
            + refreshTime);

    allSeen.put(t.getTopic(), new ArrayList<BigInteger>());

    if (this.attackType.endsWith(KademliaCommonConfig.ATTACK_TYPE_DOS)) {
      // if only a spammer than follow the normal protocol
      // super.handleInitRegisterTopic(m, myPid);
      return;
    }
    if (this.attackType.endsWith(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)) {
      HashMap<BigInteger, Long> init = new HashMap<BigInteger, Long>();
      initTicketRequestTime.put(t, init);
      // logger.warning("Log"+initTicketRequestTime.get(t).ge);
      previousTicketRequestTime.put(t, new HashMap<BigInteger, Long>());

      super.handleInitRegister(m, myPid);
    }
    if (first
        && (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_HYBRID)
            || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_MALICIOUS_REGISTRAR))) {
      first = false;
      logger.warning("Filling up the topic table with malicious entries");
      for (int i = 0; i < Network.size(); i++) {
        Node n = Network.get(i);
        KademliaProtocol prot = (KademliaProtocol) n.getKademliaProtocol();
        if (this.getNode().equals(prot.getNode())) continue; // skip this node
        if (prot.getNode().is_evil) { // add a registration to evilTopicTable
          TopicRegistration reg = new TopicRegistration(prot.getNode());
          Topic targetTopic = prot.getTargetTopic();
          ArrayList<TopicRegistration> regList = this.evilTopicTable.get(targetTopic);
          if (regList != null) regList.add(reg);
          else {
            regList = new ArrayList<TopicRegistration>();
            this.evilTopicTable.put(targetTopic, regList);
          }
        }
      }

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
    }
    super.handleInitRegister(m, myPid);

    /*
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM) || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_HYBRID) ) {

        if(!ticketTables.containsKey(t.getTopicID())) {
            KademliaObserver.addTopicRegistration(t, this.node.getId());
            int k = (int) Math.ceil((double) this.targetNumOfRegistrations / KademliaCommonConfig.NBUCKETS);
    	    TicketTable rou = new TicketTable(KademliaCommonConfig.NBUCKETS,k,10,this,t,myPid,false);
            rou.setNodeId(t.getTopicID());
            ticketTables.put(t.getTopicID(),rou);

            for(int i = 0; i<= KademliaCommonConfig.BITS;i++) {
                BigInteger[] neighbours = routingTable.getNeighbours(i);
                //if(neighbours.length!=0)logger.warning("Bucket at distance "+i+" with "+neighbours.length+" nodes");
                //else logger.warning("Bucket at distance "+i+" empty");
                this.numOfRegistrations += 1;
                rou.addNeighbour(neighbours);
            }
        }
        if (this.numOfRegistrations < this.targetNumOfRegistrations) {
            logger.warning("Failed to send " + this.targetNumOfRegistrations + " registrations - only sent " + this.numOfRegistrations);
        }
        //sendLookup(t,myPid);
    }
    else {
        super.handleInitRegisterTopic(m, myPid);
    }*/
  }

  /**
   * Process a topic query message.<br>
   * The body should contain a topic. Return a response message containing the registrations for the
   * topic and the neighbors close to the topic.
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleTopicQuery(Message m, int myPid) {

    Topic t = (Topic) m.body;
    TopicRegistration[] registrations = new TopicRegistration[0];

    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_DOS)
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)) {
      //       || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      // if only a spammer than follow the normal protocol
      super.handleTopicQuery(m, myPid);
    } else {
      ArrayList<TopicRegistration> regList = this.evilTopicTable.get(t);
      if (regList != null && !regList.isEmpty()) {
        registrations =
            (TopicRegistration[]) regList.toArray(new TopicRegistration[regList.size()]);
      }

      int result_len =
          KademliaCommonConfig.K > registrations.length
              ? registrations.length
              : KademliaCommonConfig.K;
      TopicRegistration[] final_results = new TopicRegistration[result_len];

      for (int i = 0; i < result_len; i++)
        final_results[i] = registrations[CommonState.r.nextInt(registrations.length)];

      BigInteger[] neighbours =
          this.evilRoutingTable.getNeighbours(Util.logDistance(t.getTopicID(), this.node.getId()));

      /*Message.TopicLookupBody body = new Message.TopicLookupBody(final_results, neighbours);
      Message response  = new Message(Message.MSG_TOPIC_QUERY_REPLY, body);
      response.operationId = m.operationId;
      response.src = this.node;
      response.ackId = m.id;
      logger.warning(" responds with Malicious TOPIC_QUERY_REPLY");
      sendMessage(response, m.src.getId(), myPid);*/

      if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM))
        registrations = new TopicRegistration[0];

      Message.TopicLookupBody body = new Message.TopicLookupBody(registrations, neighbours);
      Message response = new Message(Message.MSG_TOPIC_QUERY_REPLY, body);
      response.operationId = m.operationId;
      response.src = this.node;
      assert m.src != null;
      response.dest = m.src;
      response.ackId = m.id;
      logger.info(" responds with TOPIC_QUERY_REPLY");
      sendMessage(response, m.src.getId(), myPid);
    }
  }

  protected void handleResponse(Message m, int myPid) {

    BigInteger[] neighbours = (BigInteger[]) m.body;
    for (String t : allSeen.keySet())
      if (allSeen.get(t) != null) allSeen.get(t).addAll(Arrays.asList(neighbours));

    logger.warning("handleresponse " + neighbours.length);

    super.handleResponse(m, myPid);
  }

  protected void startRegistration(RegisterOperation rop, int myPid) {

    // int distToTopic = Util.logDistance((BigInteger) rop.getTopic().getTopicID(),
    // this.node.getId());
    // BigInteger[]  neighbours = this.routingTable.getNeighbours(distToTopic);

    /*for (BigInteger id : rop.getNeighboursList()) {
    	logger.warning("start reg neighbour "+id+" "+Util.logDistance(rop.getTopic().getTopicID(), id));
    }*/
    /*if(neighbours.length < KademliaCommonConfig.ALPHA)
    neighbours = this.routingTable.getKClosestNeighbours(KademliaCommonConfig.ALPHA, distToTopic);*/

    // rop.elaborateResponse(neighbours);
    // rop.available_requests = KademliaCommonConfig.ALPHA;

    // logger.warning("Start registration2 "+rop.getMessage().type+" "+rop.operationId+"
    // "+rop.getNeighboursList().size());
    Message message = rop.getMessage();
    message.operationId = rop.operationId;
    message.type = Message.MSG_REGISTER;
    message.src = this.node;

    List<BigInteger> registrars = allSeen.get(rop.getTopic().getTopic());

    logger.warning("Registrars " + registrars.size() + " " + n_regs);

    allSeen.remove(rop.getTopic().getTopic());

    rop.available_requests = KademliaCommonConfig.ALPHA;
    // send ALPHA messages

    int regs = registrars.size() > n_regs ? n_regs : registrars.size();

    for (int i = 0; i < regs; i++) {
      // BigInteger nextNode = rop.getNeighbour();
      // logger.warning("Nextnode "+nextNode);
      BigInteger nextNode = registrars.get(i);
      if (nextNode != null) {
        message.dest = new KademliaNode(nextNode);
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
          Topic t_rand = Util.generateRandomTopic(urg);
          message.body = t_rand;
        }
        sendMessage(message.copy(), nextNode, myPid);
        rop.nrHops++;
      } else {
        break; // nextNode may be null, if the node has less than ALPHA neighbours
      }
    }
  }

  /**
   * Response to a route request.<br>
   * Respond with only K-closest malicious peers
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  protected void handleFind(Message m, int myPid, int dist) {

    logger.warning("Handle find evil");
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_DOS)
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      super.handleFind(m, myPid, dist);
      return;
    }
    // get the ALPHA closest node to destNode
    this.evilRoutingTable.setNodeId(this.node.getId());
    BigInteger[] neighbours = this.evilRoutingTable.getNeighbours(dist);

    /*System.out.print("Including neigbours: [");
    for(BigInteger n : neighbours){
    	System.out.println(", " + n);
    }
    System.out.println("]");*/

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
   * Process a register response message.<br>
   * The body should contain a ticket, which indicates whether registration is complete. In case it
   * is not, schedule sending a new register request
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleRegisterResponse(Message m, int myPid) {

    TopicRegistration r = (TopicRegistration) m.body;

    Topic t = r.getTopic();
    KademliaObserver.reportActiveRegistration(t, this.node.is_evil);

    KademliaObserver.addAcceptedRegistration(
        t,
        this.node.getId(),
        m.src.getId(),
        CommonState.getTime() - r.getTimestamp(),
        this.node.is_evil);

    operations.remove(m.operationId);

    if (this.nRefresh == 1) {
      if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
        if (scheduled.get(this.targetTopic.getTopic()) != null) {
          int sch = scheduled.get(this.targetTopic.getTopic()) + 1;
          scheduled.put(this.targetTopic.getTopic(), sch);
        } else {
          scheduled.put(this.targetTopic.getTopic(), 1);
        }
      } else {
        if (scheduled.get(t.getTopic()) != null) {
          int sch = scheduled.get(t.getTopic()) + 1;
          scheduled.put(t.getTopic(), sch);
        } else {
          scheduled.put(t.getTopic(), 1);
        }
      }
    }
    Timeout timeout = new Timeout(t, m.src.getId());
    EDSimulator.add(refreshTime, timeout, Util.nodeIdtoNode(this.node.getId()), myPid);
  }

  /**
   * manage the peersim receiving of the events
   *
   * @param myNode Node
   * @param myPid int
   * @param event Object
   */
  public void processEvent(Node myNode, int myPid, Object event) {

    // super.processEvent(myNode, myPid, event);
    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;
    }

    // TODO we could simply let these "handle" calls made in the parent class
    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegister(m, myPid);
        break;

      case Message.MSG_TOPIC_QUERY:
        m = (Message) event;
        handleTopicQuery(m, myPid);
        break;

      case Timeout.REG_TIMEOUT:
        KademliaObserver.reportExpiredRegistration(((Timeout) event).topic, this.node.is_evil);
        if (KademliaCommonConfig.REG_REFRESH == 1) {
          String top = ((Timeout) event).topic.getTopic();
          if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
            top = this.targetTopic.getTopic();
          }
          int sch = scheduled.get(top) - 1;
          scheduled.put(top, sch);
          logger.info("scheduled Topic " + top + " " + sch);
          if (sch == 0) {
            logger.warning("Registering again");
            EDSimulator.add(
                0,
                generateRegisterMessage(top),
                Util.nodeIdtoNode(this.node.getId()),
                this.getProtocolID());
          }
        }
        break;

      default:
        super.processEvent(myNode, myPid, event);
    }
  }
}
