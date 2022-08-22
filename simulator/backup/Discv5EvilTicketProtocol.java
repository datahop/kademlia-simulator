package peersim.kademlia;

/** Discv5 Ticket Evil Protocol implementation. */
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.operations.TicketOperation;
import peersim.transport.UnreliableTransport;

public class Discv5EvilTicketProtocol extends Discv5TicketProtocol {

  // VARIABLE PARAMETERS
  final String PAR_ATTACK_TYPE = "attackType";
  final String PAR_NUMBER_OF_REGISTRATIONS = "numberOfRegistrations";

  UniformRandomGenerator urg = new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
  // type of attack (TopicSpam)
  private String attackType;
  // number of registrations to make
  private int numOfRegistrations;
  private int targetNumOfRegistrations;
  private boolean firstSpam = true;
  private boolean first = true;
  private HashMap<Topic, ArrayList<TopicRegistration>> evilTopicTable;
  private RoutingTable evilRoutingTable; // routing table only containing evil neighbors
  private HashMap<Topic, HashMap<BigInteger, Long>> initTicketRequestTime;
  private HashMap<Topic, HashMap<BigInteger, Long>> previousTicketRequestTime;
  private int ticketBucketSize;
  // In case of ATTACK_TYPE_TOPIC_SPAM, the target topic is only used as a destination for
  // registration msgs and random topics are registered
  private Topic targetTopic;
  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5EvilTicketProtocol dolly = new Discv5EvilTicketProtocol(Discv5EvilTicketProtocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public Discv5EvilTicketProtocol(String prefix) {
    super(prefix);
    this.ticketBucketSize =
        Configuration.getInt(
            prefix + "." + PAR_TICKET_TABLE_BUCKET_SIZE, KademliaCommonConfig.TICKET_BUCKET_SIZE);
    this.attackType = Configuration.getString(prefix + "." + PAR_ATTACK_TYPE);
    this.numOfRegistrations = 0;
    this.targetNumOfRegistrations =
        Configuration.getInt(prefix + "." + PAR_NUMBER_OF_REGISTRATIONS, 0);
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

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage(Topic t) {
    // existing active destination node

    BigInteger dst = t.topicID;

    Message m = Message.makeInitFindNode(dst);
    m.timestamp = CommonState.getTime();

    return m;
  }

  /**
   * Start a register topic operation.<br>
   * If this is an on-going register operation with a previously obtained ticket, then send a
   * REGTOPIC message; otherwise, Find the ALPHA closest node and send REGTOPIC message to them
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitRegisterTopic(Message m, int myPid) {

    Topic t = (Topic) m.body;
    this.targetTopic = getTargetTopic();

    assert (t.getTopic().equals(targetTopic.getTopic()))
        : "target topic is different from the one that is registered";

    logger.warning(
        "In handleInitRegister of EVIL "
            + t.getTopic()
            + " "
            + t.getTopicID()
            + " "
            + Configuration.getInt(
                prefix + "." + PAR_TICKET_TABLE_BUCKET_SIZE,
                KademliaCommonConfig.TICKET_BUCKET_SIZE));

    if (this.attackType.endsWith(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)) {
      HashMap<BigInteger, Long> init = new HashMap<BigInteger, Long>();
      initTicketRequestTime.put(t, init);
      // logger.warning("Log"+initTicketRequestTime.get(t).ge);
      previousTicketRequestTime.put(t, new HashMap<BigInteger, Long>());

      // super.handleInitRegisterTopic(m, myPid);
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

    // TODO check if we are using activeTopics
    activeTopics.add(t.getTopic());

    TicketTable tt =
        new TicketTable(
            KademliaCommonConfig.TTNBUCKETS,
            ticketBucketSize,
            KademliaCommonConfig.TICKET_TABLE_REPLACEMENTS,
            this,
            t,
            myPid,
            KademliaCommonConfig.TICKET_REFRESH == 1);
    tt.setNodeId(t.getTopicID());
    ticketTables.put(t.getTopicID(), tt);

    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = routingTable.getNeighbours(i);
      tt.addNeighbour(neighbours);
    }

    // if the attack type is TOPIC_SPAM (i.e., spam random topics)
    // we should do a FIND request to the target topic (say t1) that is attacked
    // in order to populate the ticket table with neighbors close to t1.
    // Otherwise, ticket requests with random topics populate the ticket table
    // with only the peers close to the random topic
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      Message mFind = generateFindNodeMessage(t);
      handleInitFind(mFind, myPid);
    }
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
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM))
    //       || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM))
    {
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

      if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
        final_results = new TopicRegistration[0];
      }

      Message.TopicLookupBody body = new Message.TopicLookupBody(final_results, neighbours);
      Message response = new Message(Message.MSG_TOPIC_QUERY_REPLY, body);
      response.operationId = m.operationId;
      response.src = this.node;
      response.ackId = m.id;
      logger.warning(" responds with Malicious TOPIC_QUERY_REPLY");
      sendMessage(response, m.src.getId(), myPid);
    }
  }

  /**
   * Process a ticket request by malicious node. The goal here is to approve all registrations
   * immediately.
   */
  protected void handleTicketRequest(Message m, int myPid) {

    logger.warning("Handle ticket request EVIL");
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_DOS)
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)
        || this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      super.handleTicketRequest(m, myPid);
      return;
    }

    long curr_time = CommonState.getTime();
    // System.out.println("Ticket request received from " + m.src.getId()+" in node
    // "+this.node.getId());
    Topic topic = (Topic) m.body;

    KademliaNode advertiser = m.src;
    // System.out.println("TicketRequest handle "+topic.getTopic());
    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    long rtt_delay =
        2
            * transport.getLatency(
                Util.nodeIdtoNode(m.src.getId()), Util.nodeIdtoNode(m.dest.getId()));
    Ticket ticket =
        ((Discv5GlobalTopicTable) this.topicTable)
            .getTicket(topic, advertiser, rtt_delay, curr_time);

    // Send a response message with evil neighbors back to the requester
    BigInteger[] neighbours =
        this.evilRoutingTable.getNeighbours(
            Util.logDistance(topic.getTopicID(), this.node.getId()));

    Message.TicketReplyBody body = new Message.TicketReplyBody(ticket, neighbours);

    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_DOS)) {
      body =
          new Message.TicketReplyBody(
              new Ticket(topic, curr_time, 100000000, advertiser, rtt_delay, 0), new BigInteger[0]);
    }
    Message response = new Message(Message.MSG_TICKET_RESPONSE, body);

    response.ackId = m.id; // set ACK number
    response.operationId = m.operationId;

    sendMessage(response, m.src.getId(), myPid);
  }

  /** Process a ticket response and schedule a register message */
  protected void handleTicketResponse(Message m, int myPid) {

    logger.info("Got response EVIL! Is topic queue full?");
    Message.TicketReplyBody body = (Message.TicketReplyBody) m.body;
    Ticket ticket = body.ticket;
    if (!(this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM))) {
      assert ticket.getTopic().getTopic().equals(targetTopic.getTopic());
    }
    TicketTable tt;
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      tt = ticketTables.get(targetTopic.getTopicID());
    } else {
      Topic topic = ticket.getTopic();
      tt = ticketTables.get(topic.getTopicID());
    }
    tt.reportResponse(ticket);

    if (KademliaCommonConfig.TICKET_NEIGHBOURS == 1) {

      for (BigInteger node : body.neighbours) routingTable.addNeighbour(node);

      if (tt != null) {
        tt.addNeighbour(body.neighbours);
      }
      SearchTable st = searchTables.get(targetTopic.getTopicID());
      if (st != null) st.addNeighbour(body.neighbours);
    }

    // if request is rejected because of re-registration attempt
    if (ticket.getWaitTime() == -1 || ticket.getCumWaitTime() > KademliaCommonConfig.REG_TIMEOUT) {

      if (ticket.getWaitTime() == -1)
        logger.warning(
            "Attempted to re-register topic on the same node "
                + m.src.getId()
                + " for topic "
                + ticket.getTopic().getTopic());
      else {
        logger.info("Ticket request cumwaitingtime too big " + ticket.getCumWaitTime());
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
          KademliaObserver.reportOverThresholdWaitingTime(
              this.targetTopic.getTopic(), this.node.getId(), m.src.getId(), ticket.getWaitTime());
        } else {
          KademliaObserver.reportOverThresholdWaitingTime(
              ticket.getTopic().getTopic(), this.node.getId(), m.src.getId(), ticket.getWaitTime());
        }
      }
      tt.removeNeighbour(m.src.getId());
      RetryTimeout timeout = new RetryTimeout(ticket.getTopic(), m.src.getId());
      EDSimulator.add(
          KademliaCommonConfig.AD_LIFE_TIME, timeout, Util.nodeIdtoNode(this.node.getId()), myPid);
      return;
    }

    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_WAITING_TIME_SPAM)) {

      logger.info("Waiting time attack");

      if (previousTicketRequestTime.get(targetTopic).get(m.src.getId()) != null) {
        if ((CommonState.getTime() - initTicketRequestTime.get(targetTopic).get(m.src.getId()) > 0)
            && (previousTicketRequestTime.get(targetTopic).get(m.src.getId())
                > (CommonState.getTime()
                    - initTicketRequestTime.get(targetTopic).get(m.src.getId())
                    + ticket.getWaitTime()
                    + ticket.getRTT()))) {
          logger.warning(
              "Received smaller waiting time than before "
                  + ticket.getWaitTime()
                  + " "
                  + previousTicketRequestTime.get(targetTopic).get(m.src.getId())
                  + " "
                  + CommonState.getTime()
                  + " "
                  + initTicketRequestTime.get(targetTopic).get(m.src.getId()));
          KademliaObserver.reportBetterWaitingTime(
              ticket.getTopic().getTopic(),
              previousTicketRequestTime.get(targetTopic).get(m.src.getId()),
              ticket.getWaitTime(),
              CommonState.getTime() - initTicketRequestTime.get(targetTopic).get(m.src.getId()));
        }
      }

      initTicketRequestTime.get(targetTopic).put(m.src.getId(), CommonState.getTime());
      previousTicketRequestTime.get(targetTopic).put(m.src.getId(), ticket.getWaitTime());
      super.sendTicketRequest(m.src.getId(), targetTopic, myPid);

    } else {

      if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
        TicketOperation top = (TicketOperation) operations.get(m.operationId);
        tt.addTicketRandomTopic(m, ticket, (Topic) top.body);
      } else {
        tt.addTicket(m, ticket);
      }
    }
  }
  /** Process register requests The malicious node simply approves all tickets */
  protected void handleRegister(Message m, int myPid) {
    super.handleRegister(m, myPid);
    /*
    Ticket ticket = (Ticket) m.body;
          ticket.setRegistrationComplete(true);

          Message response  = new Message(Message.MSG_REGISTER_RESPONSE, ticket);
          response.ackId = ticket.getMsg().id;
          response.operationId = ticket.getMsg().operationId;
          sendMessage(response, ticket.getSrc().getId(), myPid);*/
  }

  /**
   * Response to a route request.<br>
   * Respond with only K-closest malicious peers
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  protected void handleFind(Message m, int myPid, int dist) {

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

  public void sendTicketRequest(BigInteger dest, Topic t, int myPid) {
    logger.info("Sending ticket request to " + dest + " for topic " + t.topic);
    Message m;
    TicketOperation top = new TicketOperation(this.node.getId(), CommonState.getTime(), t);
    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM) && !firstSpam) {
      // Generate a ticket request for a random topic
      Topic t_rand = Util.generateRandomTopic(urg);
      top.body = t_rand;
      m = new Message(Message.MSG_TICKET_REQUEST, t_rand);
    } else {
      top.body = t;
      m = new Message(Message.MSG_TICKET_REQUEST, t);
      firstSpam = false;
    }
    operations.put(top.operationId, top);

    // Lookup the target address in the routing table
    BigInteger[] neighbours = new BigInteger[] {dest};

    top.elaborateResponse(neighbours);
    top.available_requests = KademliaCommonConfig.ALPHA;

    m.timestamp = CommonState.getTime();
    // set message operation id
    m.operationId = top.operationId;
    m.src = this.node;

    sendMessage(m, top.getNeighbour(), myPid);
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
    Ticket ticket = (Ticket) m.body;
    Topic topic = ticket.getTopic();
    if (!ticket.isRegistrationComplete()) {
      logger.info(
          "Unsuccessful Registration of topic: "
              + ticket.getTopic().getTopic()
              + " at node: "
              + m.src.getId()
              + " wait time: "
              + ticket.getWaitTime()
              + " "
              + ticket.getCumWaitTime());
      Message register = new Message(Message.MSG_REGISTER, ticket);
      register.operationId = m.operationId;
      register.body = m.body;

      BackoffService backoff = registrationFailed.get(ticket);
      if (backoff == null) {
        backoff =
            new BackoffService(
                KademliaCommonConfig.AD_LIFE_TIME, KademliaCommonConfig.MAX_REGISTRATION_RETRIES);
        backoff.registrationFailed();
        registrationFailed.put(ticket, backoff);
      } else {
        backoff.registrationFailed();
      }
      logger.info(
          "Registration failed "
              + backoff.getTimesFailed()
              + " backing off "
              + +backoff.getTimeToWait()
              + " "
              + backoff.shouldRetry()
              + " "
              + ticket.getWaitTime());

      if (backoff.shouldRetry()
          && (ticket.getWaitTime() >= 0)
          && (ticket.getCumWaitTime() <= KademliaCommonConfig.REG_TIMEOUT)) {
        scheduleSendMessage(register, m.src.getId(), myPid, ticket.getWaitTime());
      } else {
        logger.info("Ticket request cumwaitingtime too big " + ticket.getCumWaitTime());
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
          ticketTables.get(this.targetTopic.getTopicID()).removeNeighbour(m.src.getId());
        } else {
          ticketTables.get(topic.getTopicID()).removeNeighbour(m.src.getId());
        }
        RetryTimeout timeout = new RetryTimeout(ticket.getTopic(), m.src.getId());
        EDSimulator.add(
            KademliaCommonConfig.AD_LIFE_TIME,
            timeout,
            Util.nodeIdtoNode(this.node.getId()),
            myPid);

        if (ticket.getCumWaitTime() > KademliaCommonConfig.REG_TIMEOUT)
          KademliaObserver.reportOverThresholdWaitingTime(
              topic.getTopic(), this.node.getId(), m.src.getId(), ticket.getWaitTime());
      }

    } else {
      logger.warning(
          "Registration succesful for topic "
              + ticket.getTopic().topic
              + " at node "
              + m.src.getId()
              + " at dist "
              + Util.logDistance(m.src.getId(), ticket.getTopic().getTopicID())
              + " "
              + ticket.getCumWaitTime());
      KademliaObserver.addAcceptedRegistration(
          topic, this.node.getId(), m.src.getId(), ticket.getCumWaitTime(), this.node.is_evil);
      KademliaObserver.reportActiveRegistration(ticket.getTopic(), this.node.is_evil);

      Timeout timeout = new Timeout(ticket.getTopic(), m.src.getId());
      // ticketTables.get(topic.getTopicID()).removeNeighbour(m.src.getId());

      EDSimulator.add(
          KademliaCommonConfig.AD_LIFE_TIME, timeout, Util.nodeIdtoNode(this.node.getId()), myPid);

      if (this.removeAfterReg == 1) {
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
          ticketTables.get(this.targetTopic.getTopicID()).removeNeighbour(m.src.getId());
        } else {
          ticketTables.get(topic.getTopicID()).removeNeighbour(m.src.getId());
        }
      }

      if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
        ticketTables.get(this.targetTopic.getTopicID()).acceptedReg(m.src.getId());
        logger.info(
            "Active registrations "
                + ticketTables.get(this.targetTopic.getTopicID()).bucketWithRegs());
      } else {
        ticketTables.get(ticket.getTopic().getTopicID()).acceptedReg(m.src.getId());
        logger.info(
            "Active registrations "
                + ticketTables.get(ticket.getTopic().getTopicID()).bucketWithRegs());
      }

      /*if (KademliaCommonConfig.PARALLELREGISTRATIONS == 0) {

      	ticketTables.get(ticket.getTopic().getTopicID()).increaseAvailableRequests();

      	if (ticketTables.get(ticket.getTopic().getTopicID()).getAvailableRequests() > 0) {
      		BigInteger nextNode = ticketTables.get(ticket.getTopic().getTopicID()).getNeighbour();
      		if (nextNode != null) {
      			sendTicketRequest(nextNode, ticket.getTopic(), myPid);
      		}
      	}
      }*/

    }

    if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
      if (printSearchTable) ticketTables.get(this.targetTopic.getTopicID()).print();
    } else {
      if (printSearchTable) ticketTables.get(ticket.getTopic().getTopicID()).print();
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

    // super.processEvent(myNode, myPid, event);
    Message m;

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;
    }

    TicketTable tt;
    // TODO we could simply let these "handle" calls made in the parent class
    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegisterTopic(m, myPid);
        break;

      case Message.MSG_TOPIC_QUERY:
        m = (Message) event;
        handleTopicQuery(m, myPid);
        break;

      case Timeout.REG_TIMEOUT:
        logger.warning("Remove ticket table " + ((Timeout) event).nodeSrc);
        KademliaObserver.reportExpiredRegistration(((Timeout) event).topic, this.node.is_evil);
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {

          tt = ticketTables.get(this.targetTopic.getTopicID());
        } else {
          tt = ticketTables.get(((Timeout) event).topic.getTopicID());
        }
        // tt.removeNeighbour(((Timeout) event).nodeSrc);
        // }
        // ticketTables.get(((Timeout)event).topic.getTopicID()).removeRegisteredList(((Timeout)event).nodeSrc);
        tt.removeRegisteredList(((Timeout) event).nodeSrc);
        tt.expiredReg(((Timeout) event).nodeSrc);
        if (this.removeAfterReg == 0) {
          tt.removeNeighbour(((Timeout) event).nodeSrc);
        }

        break;
      case RetryTimeout.RETRY:
        logger.info("Remove ticket table " + ((RetryTimeout) event).nodeSrc);
        if (this.attackType.equals(KademliaCommonConfig.ATTACK_TYPE_TOPIC_SPAM)) {
          tt = ticketTables.get(this.targetTopic.getTopicID());
        } else {
          tt = ticketTables.get(((RetryTimeout) event).topic.getTopicID());
        }
        tt.removeRegisteredList(((RetryTimeout) event).nodeSrc);
        break;
      default:
        super.processEvent(myNode, myPid, event);
    }
  }
}
