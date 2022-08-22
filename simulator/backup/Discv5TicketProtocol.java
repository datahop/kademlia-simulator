package peersim.kademlia;

/** Discv5 Protocol implementation. */
import java.math.BigInteger;
import java.util.HashMap;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.operations.LookupTicketOperation;
import peersim.kademlia.operations.Operation;
import peersim.kademlia.operations.TicketOperation;
import peersim.transport.UnreliableTransport;

public class Discv5TicketProtocol extends Discv5Protocol {

  /** Table to keep track of topic registrations */
  protected HashMap<BigInteger, TicketTable> ticketTables;

  /** Table to search for topics */
  protected HashMap<BigInteger, SearchTable> searchTables;

  protected HashMap<Ticket, BackoffService> registrationFailed;

  // Topic table capacity
  final String PAR_TOPIC_TABLE_CAP = "TOPIC_TABLE_CAP";
  // The limit on the number of adverisements allowed to register per topic at a
  // registrar
  final String PAR_ADS_PER_QUEUE = "ADS_PER_QUEUE";
  // The lifetime of an advertisement (after which it expires and removed from
  // topic table)
  // Number of buckets in a ticket table
  final String PAR_TICKET_TABLE_BUCKET_SIZE = "TICKET_TABLE_BUCKET_SIZE";
  // Number of buckets in a search table
  final String PAR_SEARCH_TABLE_BUCKET_SIZE = "SEARCH_TABLE_BUCKET_SIZE";
  //
  final String PAR_REFRESH_TICKET_TABLE = "REFRESH_TICKET_TABLE";
  final String PAR_REFRESH_SEARCH_TABLE = "REFRESH_SEARCH_TABLE";
  final String PAR_TICKET_NEIGHBOURS = "TICKET_NEIGHBOURS";
  final String PAR_LOOKUP_BUCKET_ORDER = "LOOKUP_BUCKET_ORDER";
  final String PAR_TICKET_REMOVE_AFTER_REG = "TICKET_REMOVE_AFTER_REG";
  final String PAR_TICKET_TABLE_REPLACEMENTS = "TICKET_TABLE_REPLACEMENTS";
  final String PAR_SEARCH_TABLE_REPLACEMENTS = "SEARCH_TABLE_REPLACEMENTS";
  final String PAR_MAX_REGISTRATION_RETRIES = "MAX_REGISTRATION_RETRIES";
  // final String PAR_MAX_REG_BUCKETS = "MAX_REG_BUCKETS";
  // final String PAR_ROUND_ROBIN_TOPIC_TABLE = "ROUND_ROBIN";

  final String PAR_STOP_REGISTER_WINDOW_SIZE = "STOP_REGISTER_WINDOW_SIZE";
  final String PAR_STOP_REGISTER_MIN_REGS = "STOP_REGISTER_MIN_REGS";

  // final String PAR_PARALLELREGISTRATIONS = "PARALLELREGISTRATIONS";
  // final String PAR_SLOT = "SLOT";
  final String PAR_TTNBUCKETS = "TTNBUCKETS";
  final String PAR_STNBUCKETS = "STNBUCKETS";

  final String PAR_REGTIMEOUT = "REG_TIMEOUT";
  final String PAR_FILTERESULTS = "FILTER_RESULTS";

  boolean firstRegister;
  boolean printSearchTable = false;

  protected int removeAfterReg;
  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5TicketProtocol dolly = new Discv5TicketProtocol(Discv5TicketProtocol.prefix);
    return dolly;
  }

  /**
   * Used only by the initializer when creating the prototype. Every other instance call CLONE to
   * create the new object.
   *
   * @param prefix String
   */
  public Discv5TicketProtocol(String prefix) {
    super(prefix);
    ticketTables = new HashMap<BigInteger, TicketTable>();
    searchTables = new HashMap<BigInteger, SearchTable>();
    registrationFailed = new HashMap<Ticket, BackoffService>();

    /*if (KademliaCommonConfig.ROUND_ROBIN_TOPIC_TABLE == 1) {
    	this.topicTable = new Discv5RRTopicTable();
    } else if (KademliaCommonConfig.ROUND_ROBIN_TOPIC_TABLE == 2) {
    	this.topicTable = new Discv5GlobalTopicTable();
          }else if (KademliaCommonConfig.ROUND_ROBIN_TOPIC_TABLE == 3) {
              this.topicTable = new Discv5StatefulTopicTable();
    } else {
    	this.topicTable = new Discv5TicketTopicTable();
    }*/
    this.topicTable = new Discv5StatefulTopicTable();
    firstRegister = true;
  }

  /**
   * This procedure is called only once and allow to inizialize the internal state of
   * KademliaProtocol. Every node shares the same configuration, so it is sufficient to call this
   * routine once.
   */
  protected void _init() {
    // execute once
    if (_ALREADY_INSTALLED) return;

    KademliaCommonConfig.TOPIC_TABLE_CAP =
        Configuration.getInt(
            prefix + "." + PAR_TOPIC_TABLE_CAP, KademliaCommonConfig.TOPIC_TABLE_CAP);
    KademliaCommonConfig.ADS_PER_QUEUE =
        Configuration.getInt(prefix + "." + PAR_ADS_PER_QUEUE, KademliaCommonConfig.ADS_PER_QUEUE);

    KademliaCommonConfig.TICKET_BUCKET_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_TICKET_TABLE_BUCKET_SIZE, KademliaCommonConfig.TICKET_BUCKET_SIZE);
    KademliaCommonConfig.SEARCH_BUCKET_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_SEARCH_TABLE_BUCKET_SIZE, KademliaCommonConfig.SEARCH_BUCKET_SIZE);
    KademliaCommonConfig.TICKET_REFRESH =
        Configuration.getInt(
            prefix + "." + PAR_REFRESH_TICKET_TABLE, KademliaCommonConfig.TICKET_REFRESH);
    KademliaCommonConfig.SEARCH_REFRESH =
        Configuration.getInt(
            prefix + "." + PAR_REFRESH_SEARCH_TABLE, KademliaCommonConfig.SEARCH_REFRESH);
    KademliaCommonConfig.TICKET_NEIGHBOURS =
        Configuration.getInt(
            prefix + "." + PAR_TICKET_NEIGHBOURS, KademliaCommonConfig.TICKET_NEIGHBOURS);
    KademliaCommonConfig.LOOKUP_BUCKET_ORDER =
        Configuration.getInt(
            prefix + "." + PAR_LOOKUP_BUCKET_ORDER, KademliaCommonConfig.LOOKUP_BUCKET_ORDER);
    this.removeAfterReg =
        Configuration.getInt(
            prefix + "." + PAR_TICKET_REMOVE_AFTER_REG,
            KademliaCommonConfig.TICKET_REMOVE_AFTER_REG);
    KademliaCommonConfig.TICKET_TABLE_REPLACEMENTS =
        Configuration.getInt(
            prefix + "." + PAR_TICKET_TABLE_REPLACEMENTS,
            KademliaCommonConfig.TICKET_TABLE_REPLACEMENTS);
    KademliaCommonConfig.SEARCH_TABLE_REPLACEMENTS =
        Configuration.getInt(
            prefix + "." + PAR_SEARCH_TABLE_REPLACEMENTS,
            KademliaCommonConfig.SEARCH_TABLE_REPLACEMENTS);
    KademliaCommonConfig.MAX_REGISTRATION_RETRIES =
        Configuration.getInt(
            prefix + "." + PAR_MAX_REGISTRATION_RETRIES,
            KademliaCommonConfig.MAX_REGISTRATION_RETRIES);
    // KademliaCommonConfig.MAX_REG_BUCKETS = Configuration.getInt(prefix + "." +
    // PAR_MAX_REG_BUCKETS,
    //		KademliaCommonConfig.MAX_REG_BUCKETS);
    KademliaCommonConfig.STOP_REGISTER_WINDOW_SIZE =
        Configuration.getInt(
            prefix + "." + PAR_STOP_REGISTER_WINDOW_SIZE,
            KademliaCommonConfig.STOP_REGISTER_WINDOW_SIZE);
    KademliaCommonConfig.STOP_REGISTER_MIN_REGS =
        Configuration.getInt(
            prefix + "." + PAR_STOP_REGISTER_MIN_REGS, KademliaCommonConfig.STOP_REGISTER_MIN_REGS);
    // KademliaCommonConfig.ROUND_ROBIN_TOPIC_TABLE = Configuration.getInt(prefix + "." +
    // PAR_ROUND_ROBIN_TOPIC_TABLE,
    //		KademliaCommonConfig.ROUND_ROBIN_TOPIC_TABLE);

    /*KademliaCommonConfig.PARALLELREGISTRATIONS = Configuration.getInt(prefix + "." + PAR_PARALLELREGISTRATIONS,
    KademliaCommonConfig.PARALLELREGISTRATIONS);*/

    KademliaCommonConfig.REG_TIMEOUT =
        Configuration.getInt(prefix + "." + PAR_REGTIMEOUT, KademliaCommonConfig.REG_TIMEOUT);

    KademliaCommonConfig.FILTER_RESULTS =
        Configuration.getInt(prefix + "." + PAR_FILTERESULTS, KademliaCommonConfig.FILTER_RESULTS);

    // KademliaCommonConfig.SLOT = Configuration.getInt(prefix + "." + PAR_SLOT,
    // KademliaCommonConfig.SLOT);
    KademliaCommonConfig.TTNBUCKETS =
        Configuration.getInt(prefix + "." + PAR_TTNBUCKETS, KademliaCommonConfig.TTNBUCKETS);
    KademliaCommonConfig.STNBUCKETS =
        Configuration.getInt(prefix + "." + PAR_STNBUCKETS, KademliaCommonConfig.STNBUCKETS);
    super._init();
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
    Node src = Util.nodeIdtoNode(this.node.getId());
    Node dest = Util.nodeIdtoNode(destId);

    assert delay >= 0 : "attempting to schedule a message in the past";

    int destpid = dest.getKademliaProtocol().getProtocolID();

    m.src = this.node;
    m.dest = Util.nodeIdtoNode(destId).getKademliaProtocol().getNode();

    logger.info("-> (" + m + "/" + m.id + ") " + destId);

    // TODO: remove the assert later
    // assert(src == this.node);

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    long network_delay = transport.getLatency(src, dest);

    // if (KademliaCommonConfig.PARALLELREGISTRATIONS == 1) {

    EDSimulator.add(network_delay + delay, m, dest, destpid);
    if ((m.getType() == Message.MSG_FIND)
        || (m.getType() == Message.MSG_REGISTER)
        || (m.getType() == Message.MSG_TICKET_REQUEST)) {

      Timeout t = new Timeout(destId, m.id, m.operationId);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(delay + 4 * network_delay, t, src, myPid);
    }
    /*} else {

    	if (delay + 4 * network_delay < 0)
    		delay = Long.MAX_VALUE - (4 * network_delay);
    	if (m.getType() == Message.MSG_REGISTER
    			&& (4 * network_delay + delay) >= KademliaCommonConfig.AD_LIFE_TIME) {
    		logger.info("Not scheduling message at" + (4 * network_delay + delay) + " expiration:"
    				+ KademliaCommonConfig.AD_LIFE_TIME);
    	} else {
    		EDSimulator.add(network_delay + delay, m, dest, destpid);
    		if ((m.getType() == Message.MSG_FIND) || (m.getType() == Message.MSG_REGISTER)
    				|| (m.getType() == Message.MSG_TICKET_REQUEST)) {

    			Timeout t = new Timeout(destId, m.id, m.operationId);

    			// add to sent msg
    			this.sentMsg.put(m.id, m.timestamp);
    			EDSimulator.add(delay + 4 * network_delay, t, src, myPid);

    		}
    		if (m.getType() == Message.MSG_REGISTER)
    			logger.info("scheduling message at" + (network_delay + delay) + " expiration:"
    					+ KademliaCommonConfig.AD_LIFE_TIME);
    	}

    }*/

  }

  /**
   * send a message with current transport layer and starting the timeout timer (which is an event)
   * if the message is a request
   *
   * @param m the message to send
   * @param destId the Id of the destination node
   * @param myPid the sender Pid
   */
  public void sendMessage(Message m, BigInteger destId, int myPid) {
    // add destination to routing table
    this.routingTable.addNeighbour(destId);

    m.src = this.node;
    // m.dest = new KademliaNode(destId);
    m.dest = Util.nodeIdtoNode(destId).getKademliaProtocol().getNode();
    Node src = Util.nodeIdtoNode(this.node.getId());
    Node dest = Util.nodeIdtoNode(destId);

    int destpid = dest.getKademliaProtocol().getProtocolID();

    logger.info("-> (" + m + "/" + m.id + ") " + destId);

    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    transport.send(src, dest, m, destpid);
    KademliaObserver.msg_sent.add(1);

    if ((m.getType() == Message.MSG_FIND)
        || (m.getType() == Message.MSG_REGISTER)
        || (m.getType() == Message.MSG_TICKET_REQUEST)
        || (m.getType() == Message.MSG_TOPIC_QUERY)) {
      Timeout t = new Timeout(destId, m.id, m.operationId);
      long latency = transport.getLatency(src, dest);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(4 * latency, t, src, myPid); // set delay = 2*RTT
    }
  }

  private void makeRegisterDecision(Topic topic, int myPid) {

    long curr_time = CommonState.getTime();
    Ticket[] tickets = ((Discv5GlobalTopicTable) this.topicTable).makeRegisterDecision(curr_time);
    logger.info("makeRegisterDecision " + tickets.length);
    for (Ticket ticket : tickets) {
      Message m = ticket.getMsg();
      Message response = new Message(Message.MSG_REGISTER_RESPONSE, ticket);
      response.ackId = m.id;
      response.operationId = m.operationId;
      sendMessage(response, ticket.getSrc().getId(), myPid);
    }
  }

  /** */
  protected void handleRegister(Message m, int myPid) {
    Ticket ticket = (Ticket) m.body;
    long curr_time = CommonState.getTime();
    // boolean add_event = ((Discv5GlobalTopicTable)this.topicTable).register_ticket(ticket, m,
    // curr_time);

    ((Discv5GlobalTopicTable) this.topicTable)
        .makeRegisterDecisionForSingleTicket(ticket, curr_time);
    Message response = new Message(Message.MSG_REGISTER_RESPONSE, ticket);
    response.ackId = m.id;
    response.operationId = m.operationId;
    sendMessage(response, ticket.getSrc().getId(), myPid);

    // Setup a timeout event for the registration decision
    /*
    if (add_event) {
    	Timeout timeout = new Timeout(ticket.getTopic());
    	EDSimulator.add(KademliaCommonConfig.ONE_UNIT_OF_TIME, timeout, Util.nodeIdtoNode(this.node.getId()),
    			myPid);
    }*/
    // logger.warning("Slot time "+ CommonState.getTime()+"
    // "+(CommonState.getTime()-slotTime));
    /*
     * if(firstRegister) { logger.warning("Slot executed"); Timeout timeout = new
     * Timeout(ticket.getTopic()); EDSimulator.add(KademliaCommonConfig.SLOT,
     * timeout, Util.nodeIdtoNode(this.node.getId()), myPid); firstRegister=false; }
     */
    // slotTime = CommonState.getTime();

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
    TopicRegistration[] registrations = this.topicTable.getRegistration(t, m.src);
    BigInteger[] neighbours =
        this.routingTable.getNeighbours(Util.logDistance(t.getTopicID(), this.node.getId()));

    Message.TopicLookupBody body;
    body = new Message.TopicLookupBody(registrations, neighbours);
    // if(registrations.length>2)body = new Message.TopicLookupBody(registrations, neighbours);
    // else body = new Message.TopicLookupBody(new TopicRegistration[0], neighbours);
    Message response = new Message(Message.MSG_TOPIC_QUERY_REPLY, body);
    response.operationId = m.operationId;
    response.src = this.node;
    response.ackId = m.id;
    logger.info(" responds with TOPIC_QUERY_REPLY");
    sendMessage(response, m.src.getId(), myPid);
  }

  /** Process a ticket request */
  protected void handleTicketRequest(Message m, int myPid) {
    // FIXME add logs
    long curr_time = CommonState.getTime();
    // System.out.println("Ticket request received from " + m.src.getId()+" in node
    // "+this.node.getId());
    Topic topic = (Topic) m.body;
    KademliaNode advertiser = m.src; // new KademliaNode(m.src);
    // logger.warning("TicketRequest handle "+m.src);
    transport = (UnreliableTransport) (Network.prototype).getProtocol(tid);
    long rtt_delay =
        2
            * transport.getLatency(
                Util.nodeIdtoNode(m.src.getId()), Util.nodeIdtoNode(m.dest.getId()));
    Ticket ticket =
        ((Discv5GlobalTopicTable) this.topicTable)
            .getTicket(topic, advertiser, rtt_delay, curr_time);
    // Send a response message with a ticket back to advertiser
    BigInteger[] neighbours =
        this.routingTable.getNeighbours(Util.logDistance(topic.getTopicID(), this.node.getId()));

    Message.TicketReplyBody body = new Message.TicketReplyBody(ticket, neighbours);
    Message response = new Message(Message.MSG_TICKET_RESPONSE, body);

    // Message response = new Message(Message.MSG_TICKET_RESPONSE, ticket);
    response.ackId = m.id; // set ACK number
    response.operationId = m.operationId;

    sendMessage(response, m.src.getId(), myPid);
  }

  /** Process a ticket response and schedule a register message */
  protected void handleTicketResponse(Message m, int myPid) {
    Message.TicketReplyBody body = (Message.TicketReplyBody) m.body;
    Ticket ticket = body.ticket;
    logger.info(
        "Got response! Is topic queue full?" + ticket.getOccupancy() + " " + ticket.getWaitTime());
    Topic topic = ticket.getTopic();
    TicketTable tt = ticketTables.get(topic.getTopicID());
    tt.reportResponse(ticket);

    if (KademliaCommonConfig.TICKET_NEIGHBOURS == 1) {

      for (BigInteger node : body.neighbours) routingTable.addNeighbour(node);

      if (tt != null) {
        tt.addNeighbour(body.neighbours);
      }
      SearchTable st = searchTables.get(topic.getTopicID());
      if (st != null) st.addNeighbour(body.neighbours);
    }

    if (ticket.getWaitTime() == -1 || ticket.getCumWaitTime() > KademliaCommonConfig.REG_TIMEOUT) {

      if (ticket.getWaitTime() == -1)
        logger.warning(
            "Attempted to re-register topic on the same node "
                + m.src.getId()
                + " for topic "
                + topic.getTopic());
      else {
        logger.info("Ticket request cumwaitingtime too big " + ticket.getCumWaitTime());
        KademliaObserver.reportOverThresholdWaitingTime(
            topic.getTopic(), this.node.getId(), m.src.getId(), ticket.getWaitTime());
      }
      tt.removeNeighbour(m.src.getId());
      RetryTimeout timeout = new RetryTimeout(ticket.getTopic(), m.src.getId());
      EDSimulator.add(
          KademliaCommonConfig.AD_LIFE_TIME, timeout, Util.nodeIdtoNode(this.node.getId()), myPid);
      /*if (KademliaCommonConfig.PARALLELREGISTRATIONS == 0) {
      	ticketTables.get(ticket.getTopic().getTopicID()).increaseAvailableRequests();

      	if (ticketTables.get(ticket.getTopic().getTopicID()).getAvailableRequests() > 0) {
      		BigInteger nextNode = ticketTables.get(ticket.getTopic().getTopicID()).getNeighbour();
      		if (nextNode != null) {
      			sendTicketRequest(nextNode, ticket.getTopic(), myPid);
      		}
      	}
      }*/
      return;
    }

    tt.addTicket(m, ticket);
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

      if ((backoff.shouldRetry() && ticket.getWaitTime() >= 0)
          && (ticket.getCumWaitTime() <= KademliaCommonConfig.REG_TIMEOUT)) {
        scheduleSendMessage(register, m.src.getId(), myPid, ticket.getWaitTime());
      } else {
        logger.info(
            "Ticket request cumwaitingtime too big "
                + ticket.getCumWaitTime()
                + " or too many tries "
                + backoff.getTimesFailed());
        ticketTables.get(topic.getTopicID()).removeNeighbour(m.src.getId());
        // ticketTables.get(topic.getTopicID()).removeRegisteredList(m.src.getId());
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
        ticketTables.get(topic.getTopicID()).removeNeighbour(m.src.getId());
      }

      ticketTables.get(ticket.getTopic().getTopicID()).acceptedReg(m.src.getId());
      logger.info(
          "Active registrations "
              + ticketTables.get(ticket.getTopic().getTopicID()).bucketWithRegs());

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

    if (printSearchTable) ticketTables.get(ticket.getTopic().getTopicID()).print();
  }

  protected void handleTopicQueryReply(Message m, int myPid) {
    LookupTicketOperation lop = (LookupTicketOperation) this.operations.get(m.operationId);
    if (lop == null) {
      return;
    }

    Message.TopicLookupBody lookupBody = (Message.TopicLookupBody) m.body;
    BigInteger[] neighbours = lookupBody.neighbours;
    TopicRegistration[] registrations = lookupBody.registrations;
    // System.out.println("Topic query reply for "+lop.operationId +" with " +
    // registrations.length+ " replies "+lop.available_requests);
    logger.info(
        " Asked node from dist:"
            + Util.logDistance(lop.topic.topicID, m.src.getId())
            + ": "
            + m.src.getId()
            + " regs:"
            + registrations.length
            + " neigh:"
            + neighbours.length
            + "-> ");
    for (BigInteger neighbour : neighbours) {
      logger.info(Util.logDistance(neighbour, lop.topic.topicID) + ", ");
    }

    lop.elaborateResponse(neighbours);
    lop.increaseReturned(m.src.getId());
    lop.addAskedNode(m.src.getId());
    if (!lop.finished) lop.increaseUsed(m.src.getId());

    /*for (SearchTable st : searchTables.values()) {
      st.addNeighbour(neighbours);
    }*/

    for (BigInteger neighbour : neighbours) {
      routingTable.addNeighbour(neighbour);

      for (TicketTable tt : ticketTables.values()) tt.addNeighbour(neighbour);
    }

    if (m.src.is_evil) lop.increaseMaliciousQueries();

    int numEvilRegs = 0;
    for (TopicRegistration r : registrations) {

      // if(!lop.getDiscovered().containsKey(r.getNode()))
      //	sendHandShake(r.getNode(),r.getTopic().getTopic(),m.operationId,myPid);

      lop.addDiscovered(r.getNode());
      KademliaObserver.addDiscovered(lop.topic, m.src.getId(), r.getNode().getId());

      if (!m.src.is_evil && r.getNode().is_evil) numEvilRegs++;
    }
    // Report occurence of honest registrar returning only evil ads
    if (numEvilRegs > 0 && (registrations.length == numEvilRegs)) lop.increaseMalRespFromHonest();

    int found = lop.discoveredCount();

    int all = KademliaObserver.topicRegistrationCount(lop.topic.getTopic());
    // int required = Math.min(all, KademliaCommonConfig.TOPIC_PEER_LIMIT);
    int required = KademliaCommonConfig.TOPIC_PEER_LIMIT;

    // if (!lop.finished && found >= required) {

    if (!lop.finished && lop.completed()) {
      logger.warning(
          "Found "
              + found
              + " registrations out of required "
              + required
              + "("
              + all
              + ") for topic "
              + lop.topic.topic
              + " after consulting "
              + lop.getUsedCount()
              + " nodes.");
      // We should never use less than 2 hops
      // assert lop.getUsedCount() > 1;
      lop.finished = true;
    }

    while ((lop.available_requests > 0)) { // I can send a new find request
      // get an available neighbour
      // System.out.println("Found "+lop.finished+" "+lop.available_requests);

      if (lop.available_requests < KademliaCommonConfig.ALPHA) {
        return;

      } else if ((lop.finished && lop.available_requests == KademliaCommonConfig.ALPHA)
          || KademliaCommonConfig.MAX_SEARCH_HOPS
              == lop.getUsedCount()) { // no new neighbour and no outstanding
        // requests
        // search operation finished
        operations.remove(lop.operationId);
        KademliaObserver.reportOperation(lop);
        if (!lop.finished) {
          logger.warning(
              "Found only "
                  + found
                  + " registrations out of "
                  + all
                  + " for topic "
                  + lop.topic.topic
                  + " after consulting "
                  + lop.getUsedCount()
                  + " nodes.");
        }
        KademliaObserver.register_total.add(all);
        KademliaObserver.register_ok.add(found);
        // FIXME
        // node.setLookupResult(lop.getDiscovered(),lop.topic.getTopic());

        if (printSearchTable) searchTables.get(lop.topic.getTopicID()).print();

        return;
      } else {
        BigInteger neighbour = lop.getNeighbour();
        if (neighbour != null) {

          // send a new request only if we didn't find the node already
          Message request = new Message(Message.MSG_TOPIC_QUERY);
          request.operationId = lop.operationId;
          request.src = this.node;
          request.body = lop.body;
          request.dest =
              Util.nodeIdtoNode(neighbour)
                  .getKademliaProtocol()
                  .getNode(); // new KademliaNode(neighbour);

          if (request != null) {
            lop.nrHops++;
            sendMessage(request, neighbour, myPid);
          }
        } else {
          lop.available_requests++;
          return;
        }
      }
    }
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
            + this.node.is_evil);
    // restore the IF statement
    KademliaObserver.addTopicRegistration(t, this.node.getId());

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
      BigInteger[] neighbours = routingTable.getNeighbours(i);
      tt.addNeighbour(neighbours);
    }
    if (printSearchTable) tt.print();

    /*if (KademliaCommonConfig.PARALLELREGISTRATIONS == 0) {
    	for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {

    		BigInteger nextNode = tt.getNeighbour();
    		if (nextNode != null) {
    			sendTicketRequest(nextNode, t, myPid);
    		}
    	}

    	// restart the process after expiry
    	EDSimulator.add(KademliaCommonConfig.AD_LIFE_TIME, m, Util.nodeIdtoNode(this.node.getId()), myPid);

    }*/

  }

  /**
   * Start a topic query opearation.<br>
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  private void handleInitTopicLookup(Message m, int myPid) {
    Topic t = (Topic) m.body;

    logger.warning("Send init lookup for topic " + this.node.getId() + " " + t.getTopic());
    // FIXME why TTNBUCKETS not STNBUCKETS ?
    SearchTable rou =
        new SearchTable(
            KademliaCommonConfig.TTNBUCKETS,
            KademliaCommonConfig.SEARCH_BUCKET_SIZE,
            KademliaCommonConfig.SEARCH_TABLE_REPLACEMENTS,
            this,
            t,
            myPid,
            KademliaCommonConfig.SEARCH_REFRESH == 1);
    rou.setNodeId(t.getTopicID());
    searchTables.put(t.getTopicID(), rou);

    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = routingTable.getNeighbours(i);
      if (neighbours.length != 0) rou.addNeighbour(neighbours);
    }

    /*
     * Message message = Message.makeInitFindNode(t.getTopicID()); message.timestamp
     * = CommonState.getTime();
     *
     * EDSimulator.add(0, message, Util.nodeIdtoNode(this.node.getId()), myPid);
     */

    if (printSearchTable) rou.print();
    sendTopicLookup(m, t, myPid);
  }

  public void sendLookup(BigInteger node, int myPid) {
    Message message = Message.makeInitFindNode(node);
    message.timestamp = CommonState.getTime();

    EDSimulator.add(0, message, Util.nodeIdtoNode(this.node.getId()), myPid);
    // System.out.println("Send init lookup to node "+Util.logDistance(node,
    // this.getNode().getId()));

  }

  public void sendTopicLookup(Message m, Topic t, int myPid) {

    KademliaObserver.lookup_total.add(1);

    LookupTicketOperation lop =
        new LookupTicketOperation(
            this.node.getId(), this.searchTables.get(t.getTopicID()), m.timestamp, t);
    lop.body = m.body;
    operations.put(lop.operationId, lop);

    m.operationId = lop.operationId;
    m.type = Message.MSG_TOPIC_QUERY;
    m.src = this.node;

    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = lop.getNeighbour();
      if (nextNode != null) {
        m.dest =
            Util.nodeIdtoNode(nextNode)
                .getKademliaProtocol()
                .getNode(); // new KademliaNode(nextNode);
        sendMessage(m.copy(), nextNode, myPid);
        // System.out.println("Send topic lookup to: " + nextNode +" at
        // distance:"+Util.logDistance(lop.topic.topicID, nextNode));
        lop.nrHops++;
      }
    }
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
    if (m.getType() == Message.MSG_RESPONSE) {
      BigInteger[] neighbours = (BigInteger[]) m.body;
      // if(neighbours.length!=0)logger.warning("Find response received at
      // "+this.node.getId()+" from "+m.src.getId()+" with "+neighbours.length+"
      // neighbours");
      for (SearchTable table : searchTables.values()) table.addNeighbour(neighbours);

      for (TicketTable table : ticketTables.values()) table.addNeighbour(neighbours);
    }
    super.handleResponse(m, myPid);
  }

  public void sendTicketRequest(BigInteger dest, Topic t, int myPid) {
    logger.info("Sending ticket request to " + dest + " for topic " + t.topic);
    TicketOperation top = new TicketOperation(this.node.getId(), CommonState.getTime(), t);
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

  }

  private void handleTimeout(Timeout t, int myPid) {
    Operation op = this.operations.get(t.opID);
    if (op != null) {
      // logger.warning("Timeout "+t.getType());
      BigInteger unavailableNode = t.node;
      if (op.type == Message.MSG_TOPIC_QUERY) {
        Message m = new Message();
        m.operationId = op.operationId;
        m.type = Message.MSG_TOPIC_QUERY_REPLY;
        m.src =
            Util.nodeIdtoNode(unavailableNode)
                .getKademliaProtocol()
                .getNode(); // new KademliaNode(unavailableNode);
        m.dest = this.node;
        m.ackId = t.msgID;
        m.body = new Message.TopicLookupBody(new TopicRegistration[0], new BigInteger[0]);
        handleTopicQueryReply(m, myPid);
      }
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

    // this.discv5id = myPid;
    if (topicTable == null) return;
    super.processEvent(myNode, myPid, event);
    Message m;

    if (!myNode.isUp()) {
      System.out.println("Removed nodes are receiving traffic");
      System.exit(1);
    }

    /*if (((SimpleEvent) event).getType() == Timeout.TIMEOUT) {
    	handleTimeout((Timeout) event, myPid);
    	return;
    }*/

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;
    }

    switch (((SimpleEvent) event).getType()) {
      case Message.MSG_TOPIC_QUERY_REPLY:
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleTopicQueryReply(m, myPid);
        break;

      case Message.MSG_REGISTER:
        m = (Message) event;
        handleRegister(m, myPid);
        break;

      case Message.MSG_REGISTER_RESPONSE:
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleRegisterResponse(m, myPid);
        break;

      case Message.MSG_TOPIC_QUERY:
        m = (Message) event;
        handleTopicQuery(m, myPid);
        break;

      case Message.MSG_INIT_TOPIC_LOOKUP:
        m = (Message) event;
        handleInitTopicLookup(m, myPid);
        break;

      case Message.MSG_INIT_REGISTER:
        m = (Message) event;
        handleInitRegisterTopic(m, myPid);
        break;

      case Message.MSG_TICKET_REQUEST:
        m = (Message) event;
        handleTicketRequest(m, myPid);
        break;

      case Message.MSG_TICKET_RESPONSE:
        m = (Message) event;
        sentMsg.remove(m.ackId);
        handleTicketResponse(m, myPid);
        break;

      case Timeout.TICKET_TIMEOUT:
        Topic t = ((Timeout) event).topic;
        makeRegisterDecision(t, myPid);
        // EDSimulator.add(KademliaCommonConfig.SLOT, (Timeout)event,
        // Util.nodeIdtoNode(this.node.getId()), myPid);
        break;

      case Timeout.REG_TIMEOUT:
        logger.warning("Remove ticket table " + ((Timeout) event).nodeSrc);
        KademliaObserver.reportExpiredRegistration(((Timeout) event).topic, this.node.is_evil);
        TicketTable tt = ticketTables.get(((Timeout) event).topic.getTopicID());
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
        tt = ticketTables.get(((RetryTimeout) event).topic.getTopicID());
        tt.removeRegisteredList(((RetryTimeout) event).nodeSrc);
        break;

      case Timeout.TIMEOUT: // timeout
        Timeout timeout = (Timeout) event;
        if (sentMsg.containsKey(timeout.msgID)) { // the response msg didn't arrived
          logger.warning(
              "Node "
                  + this.node.getId()
                  + " received a timeout: "
                  + timeout.msgID
                  + " from: "
                  + timeout.node);
          // remove form sentMsg
          sentMsg.remove(timeout.msgID);
          // FIXME handleTimeout seems to be re-delivering messages that cause repeated delivery
          // failures
          handleTimeout((Timeout) event, myPid);
        }
        break;
    }
  }

  /**
   * set the current NodeId
   *
   * @param tmp BigInteger
   */
  public void setNode(KademliaNode node) {
    ((Discv5GlobalTopicTable) this.topicTable).setHostID(node.getId());
    super.setNode(node);
  }

  /** Check nodes and replace buckets with valid nodes from replacement list */
  public void refreshBuckets() {

    if (topicTable == null) return;
    for (TicketTable ttable : ticketTables.values()) ttable.refreshBuckets();
    for (SearchTable stable : searchTables.values()) stable.refreshBuckets();
    // stable.refreshBuckets(kademliaid, otherProtocolId);

    this.routingTable.refreshBuckets();
    // this.routingTable.refreshBuckets(kademliaid, otherProtocolId);

  }

  // public void refreshBucket(TicketTable rou, BigInteger node, int distance) {
  public void refreshBucket(RoutingTable rou, int distance) {

    for (int i = 0; i <= KademliaCommonConfig.BITS; i++) {
      BigInteger[] neighbours = routingTable.getNeighbours(i);
      // logger.warning("RefreshBucket "+neighbours.length+" "+i+" "+distance);
      for (BigInteger node : neighbours) {
        if (Util.logDistance(rou.getNodeId(), node) == distance) {
          // logger.warning("RefreshBucket add neighbour "+node);
          if (rou instanceof TicketTable) ((TicketTable) rou).addNeighbour(node);
          if (rou instanceof SearchTable) ((SearchTable) rou).addNeighbour(node);
        }
      }
    }

    // BigInteger[] neighbours = routingTable.getNeighbours(Util.logDistance(node,
    // this.getNode().getId()));
    // BigInteger[] neighbours = routingTable.getNeighbours(Util.logDistance(rou.getNodeId(),
    // this.getNode().getId()));
    // logger.info("Refresh bucket adding " + neighbours.length);
    // rou.addNeighbour(neighbours);
    if (printSearchTable && rou instanceof TicketTable) ((TicketTable) rou).print();
  }

  public void onKill() {
    // System.out.println("Node removed");
    topicTable = null;
    ticketTables = null;
    searchTables = null;
    routingTable = null;
    operations = null;
  }
}
