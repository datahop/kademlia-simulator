package peersim.kademlia;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.kademlia.operations.LookupOperation;
import peersim.kademlia.operations.Operation;
import peersim.kademlia.operations.RegisterOperation;
import peersim.kademlia.operations.TicketOperation;
import peersim.transport.UnreliableTransport;

public class Discv5DHTProtocol extends Discv5Protocol {

  final String PAR_TOPIC_TABLE_CAP = "TOPIC_TABLE_CAP";
  final String PAR_N = "N_REGS";
  final String PAR_REG_REFRESH = "REG_REFRESH";
  final String PAR_REGTIMEOUT = "REG_TIMEOUT";

  private HashMap<Long, Long> registrationMap;

  protected HashMap<String, Integer> scheduled;

  protected int nRefresh;

  public Discv5DHTProtocol(String prefix) {
    super(prefix);
    this.topicTable = new Discv5TopicTable();
    this.registrationMap = new HashMap<>();
    this.scheduled = new HashMap<>();

    // TODO Auto-generated constructor stub
  }

  /**
   * Replicate this object by returning an identical copy.<br>
   * It is called by the initializer and do not fill any particular field.
   *
   * @return Object
   */
  public Object clone() {
    Discv5DHTProtocol dolly = new Discv5DHTProtocol(Discv5DHTProtocol.prefix);
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

    KademliaCommonConfig.TOPIC_TABLE_CAP =
        Configuration.getInt(
            prefix + "." + PAR_TOPIC_TABLE_CAP, KademliaCommonConfig.TOPIC_TABLE_CAP);

    KademliaCommonConfig.N = Configuration.getInt(prefix + "." + PAR_N, KademliaCommonConfig.N);

    this.nRefresh =
        Configuration.getInt(prefix + "." + PAR_REG_REFRESH, KademliaCommonConfig.REG_REFRESH);

    KademliaCommonConfig.REG_TIMEOUT =
        Configuration.getInt(prefix + "." + PAR_REGTIMEOUT, KademliaCommonConfig.REG_TIMEOUT);

    super._init();
  }

  protected void handleTopicQueryReply(Message m, int myPid) {
    LookupOperation lop = (LookupOperation) this.operations.get(m.operationId);
    if (lop == null) {
      return;
    }

    BigInteger[] neighbours = ((Message.TopicLookupBody) m.body).neighbours;
    TopicRegistration[] registrations = ((Message.TopicLookupBody) m.body).registrations;
    lop.elaborateResponse(neighbours);
    for (BigInteger neighbour : neighbours) routingTable.addNeighbour(neighbour);

    if (m.src.is_evil) lop.increaseMaliciousQueries();

    int numEvilRegs = 0;
    for (TopicRegistration r : registrations) {
      // KademliaObserver.addDiscovered(lop.topic, this.node.getId(), r.getNode().getId());

      /*if(!lop.getDiscovered().containsKey(r.getNode()))
      sendHandShake(r.getNode(),r.getTopic().getTopic(),m.operationId,myPid);*/
      KademliaObserver.addDiscovered(lop.topic, m.src.getId(), r.getNode().getId());
      lop.addDiscovered(r.getNode());

      if (!m.src.is_evil && r.getNode().is_evil) numEvilRegs++;
    }
    // Report occurence of honest registrar returning only evil ads
    if (numEvilRegs > 0 && (registrations.length == numEvilRegs)) lop.increaseMalRespFromHonest();

    lop.increaseReturned(m.src.getId());
    lop.addAskedNode(m.src.getId());
    if (!lop.finished) lop.increaseUsed(m.src.getId());

    // System.out.println("Topic query reply received for "+lop.topic.getTopic()+"
    // "+this.getNode().getId()+" "+lop.discoveredCount()+" "+lop.getUsedCount()+"
    // "+lop.getReturnedCount());

    int found = lop.discoveredCount();
    int all = KademliaObserver.topicRegistrationCount(lop.topic.topic);
    int required =
        KademliaCommonConfig
            .TOPIC_PEER_LIMIT; // Math.min(all, KademliaCommonConfig.TOPIC_PEER_LIMIT);
    if (!lop.finished && found >= required)
    // if(!lop.finished && Arrays.asList(neighbours).contains(lop.destNode))
    {
      logger.warning(
          "Found "
              + found
              + " registrations out of required "
              + required
              + "("
              + all
              + ") for topic "
              + lop.topic.topic);
      lop.finished = true;
    }

    while ((lop.available_requests > 0)) { // I can send a new find request
      // get an available neighbour
      BigInteger neighbour = lop.getNeighbour();
      if (neighbour != null) {
        if (!lop.finished) {
          // send a new request only if we didn't find the node already
          Message request = new Message(Message.MSG_REGISTER);
          request.operationId = lop.operationId;
          request.type = Message.MSG_TOPIC_QUERY;
          request.src = this.node;
          request.body = lop.body;
          request.dest = new KademliaNode(neighbour);

          if (request != null) {
            lop.nrHops++;
            sendMessage(request, neighbour, myPid);
          }
        } else {
          // getNeighbour decreases available_requests, but we didn't send a message
          lop.available_requests++;
        }

      } else if (lop.available_requests
          == KademliaCommonConfig.ALPHA) { // no new neighbour and no outstanding requests
        // search operation finished
        operations.remove(lop.operationId);
        // lop.visualize();
        logger.info("reporting operation " + lop.operationId);
        KademliaObserver.reportOperation(lop);
        // lop.visualize(); uncomment if you want to see visualization of the operation
        if (!lop.finished) {
          logger.warning(
              "Found only "
                  + found
                  + " registrations out of "
                  + all
                  + " for topic "
                  + lop.topic.topic);
        }
        // System.out.println("Writing stats");
        KademliaObserver.register_total.add(all);
        KademliaObserver.register_ok.add(found);
        // FIXME
        // node.setLookupResult(lop.getDiscovered(),lop.topic.getTopic());
        return;
      } else { // no neighbour available but exists oustanding request to wait
        return;
      }
    }
  }

  private void handleInitTopicLookup(Message m, int myPid) {
    KademliaObserver.lookup_total.add(1);

    Topic t = (Topic) m.body;

    // System.out.println("Send topic lookup for topic "+t.getTopic());

    LookupOperation lop = new LookupOperation(this.node.getId(), m.timestamp, t);
    lop.body = m.body;
    lop.type = Message.MSG_TOPIC_QUERY;
    operations.put(lop.operationId, lop);

    /*int distToTopic = Util.logDistance((BigInteger) t.getTopicID(), this.node.getId());

    logger.warning("Send init lookup for topic " + this.node.getId() + " " + t.getTopic()+" "+distToTopic);

    BigInteger[] neighbours = this.routingTable.getNeighbours(distToTopic);


    if(neighbours.length<KademliaCommonConfig.ALPHA)
    	neighbours = this.routingTable.getKClosestNeighbours(KademliaCommonConfig.ALPHA, distToTopic);*/

    BigInteger[] neighbours =
        this.routingTable.getKClosestNeighbours(KademliaCommonConfig.K, t.getTopicID());

    lop.elaborateResponse(neighbours);
    lop.available_requests = KademliaCommonConfig.ALPHA;

    // set message operation id
    m.operationId = lop.operationId;
    m.type = Message.MSG_TOPIC_QUERY;
    m.src = this.node;

    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.ALPHA; i++) {
      BigInteger nextNode = lop.getNeighbour();
      if (nextNode != null) {
        m.dest = new KademliaNode(nextNode);
        sendMessage(m.copy(), nextNode, myPid);
        lop.nrHops++;
      }
    }
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
   * Start a register opearation.<br>
   * Find the ALPHA closest node and send register request to them.
   *
   * @param m Message received (contains the node to find)
   * @param myPid the sender Pid
   */
  protected void handleInitRegister(Message m, int myPid) {

    Topic t = (Topic) m.body;
    TopicRegistration r = new TopicRegistration(this.node, t);
    // logger.warning("Sending topic registration for topic "+t.getTopic());

    activeTopics.add(t.getTopic());

    logger.warning("handleInitRegisterTopic " + t.getTopic() + " " + t.getTopicID());

    KademliaObserver.addTopicRegistration(t, this.node.getId());

    RegisterOperation rop = new RegisterOperation(this.node.getId(), m.timestamp, t, r);
    rop.body = m.body;
    rop.type = Message.MSG_REGISTER;
    operations.put(rop.operationId, rop);

    rop.setMessage(m);

    // send message
    Message mFind = generateFindNodeMessage(t);

    long op = handleInitFind(mFind, myPid);

    registrationMap.put(op, rop.operationId);

    logger.info("Registration1 operation id " + rop.operationId + " " + op);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a register message, by selecting randomly the destination.
   *
   * @return Message
   */
  protected Message generateRegisterMessage(String topic) {
    Topic t = new Topic(topic);
    Message m = Message.makeRegister(t);
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

    // add message source to my routing table

    logger.warning("handle response");
    Operation op = (Operation) this.operations.get(m.operationId);
    if (op == null) {
      // logger.warning("Operation null "+m.operationId);
      return;
    }

    BigInteger[] neighbours = (BigInteger[]) m.body;

    op.elaborateResponse(neighbours);
    for (BigInteger neighbour : neighbours) {
      /*if(registrationMap.get(op.operationId)!=null) {
      	RegisterOperation rop = (RegisterOperation) operations.get(registrationMap.get(op.operationId));
      	int distToTopic = Util.logDistance((BigInteger) rop.getTopic().getTopicID(), this.node.getId());

      	logger.warning("Found neigbour "+neighbour+" "+distToTopic+" "+Util.nodeIdtoNode(neighbour).getKademliaProtocol().getNode().is_evil);

      }*/
      routingTable.addNeighbour(neighbour);
    }

    op.increaseReturned(m.src.getId());
    if (!op.finished) op.increaseUsed(m.src.getId());

    if (!op.finished && Arrays.asList(neighbours).contains(op.destNode)) {
      logger.warning("Found node " + op.destNode);
      op.finished = true;

      if (registrationMap.get(op.operationId) != null) {
        RegisterOperation rop =
            (RegisterOperation) operations.get(registrationMap.get(op.operationId));
        rop.elaborateResponse(op.getNeighboursList().toArray(new BigInteger[0]));
        startRegistration(rop, myPid);
        registrationMap.remove(op.operationId);
      }
      KademliaObserver.find_ok.add(1);
      return;
    }

    while ((op.available_requests > 0)) { // I can send a new find request
      BigInteger neighbour = op.getNeighbour();

      if (neighbour != null) {
        if (!op.finished) {
          // send a new request only if we didn't find the node already
          Message request = null;
          if (op.type == Message.MSG_FIND) {
            request = new Message(Message.MSG_FIND);
            // request.body = Util.prefixLen(op.destNode, neighbour);
            // System.out.println("Request body distance "+Util.prefixLen(op.destNode, neighbour)+"
            // "+Util.logDistance(op.destNode, neighbour));
            request.body = Util.logDistance(op.destNode, neighbour);
          } else if (op.type == Message.MSG_REGISTER) {
            request = new Message(Message.MSG_REGISTER);
            request.body = op.body;
          } else if (op.type == Message.MSG_TICKET_REQUEST) {
            request = new Message(Message.MSG_TICKET_REQUEST);
            request.body = op.body;
          }

          if (request != null) {
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
        // op.visualize();
        /*System.out.println("###################Operaration  finished");
        if(!op.finished && op.type == Message.MSG_FIND){
        	logger.warning("Couldn't find node " + op.destNode);
        }*/
        logger.info("Finished lookup node " + op.getUsedCount());
        logger.warning(
            "Registration operation id "
                + registrationMap.get(op.operationId)
                + " "
                + op.operationId);

        KademliaObserver.reportOperation(op);

        if (registrationMap.get(op.operationId) != null) {
          RegisterOperation rop =
              (RegisterOperation) operations.get(registrationMap.get(op.operationId));
          rop.elaborateResponse(op.getNeighboursList().toArray(new BigInteger[0]));
          startRegistration(rop, myPid);
          registrationMap.remove(op.operationId);
        }
        return;

      } else { // no neighbour available but exists outstanding request to wait for
        return;
      }
    }
  }

  public void sendRegisterMessage(BigInteger dest, Topic t, int myPid) {
    logger.info("Sending ticket request to " + dest + " for topic " + t.topic);
    TicketOperation top = new TicketOperation(this.node.getId(), CommonState.getTime(), t);
    top.body = t;

    top.available_requests = KademliaCommonConfig.ALPHA;

    Message m = new Message(Message.MSG_TICKET_REQUEST, t);

    m.timestamp = CommonState.getTime();
    // set message operation id
    m.operationId = top.operationId;
    m.src = this.node;
    m.dest = Util.nodeIdtoNode(dest).getKademliaProtocol().getNode();

    logger.info("Send ticket request to " + dest + " for topic " + t.getTopic());
    sendMessage(m, dest, myPid);
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

    logger.warning(
        "Start registration "
            + rop.getMessage().type
            + " "
            + rop.operationId
            + " "
            + rop.getNeighboursList().size());
    Message message = rop.getMessage();
    message.operationId = rop.operationId;
    message.type = Message.MSG_REGISTER;
    message.src = this.node;

    rop.available_requests = KademliaCommonConfig.ALPHA;
    // send ALPHA messages
    for (int i = 0; i < KademliaCommonConfig.N; i++) {
      BigInteger nextNode = rop.getNeighbour();
      // System.out.println("Nextnode "+nextNode);
      if (nextNode != null) {
        message.dest = new KademliaNode(nextNode);
        sendMessage(message.copy(), nextNode, myPid);
        rop.nrHops++;
      } // nextNode may be null, if the node has less than ALPHA neighbours
    }
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

    EDSimulator.add(network_delay + delay, m, dest, destpid);
    if ((m.getType() == Message.MSG_FIND)
        || (m.getType() == Message.MSG_REGISTER)
        || (m.getType() == Message.MSG_TICKET_REQUEST)) {

      Timeout t = new Timeout(destId, m.id, m.operationId);

      // add to sent msg
      this.sentMsg.put(m.id, m.timestamp);
      EDSimulator.add(delay + 4 * network_delay, t, src, myPid);
    }
  }

  /**
   * Response to a register request.<br>
   * Tries to register the requesting node under the specified topic
   *
   * @param m Message
   * @param myPid the sender Pid
   */
  protected void handleRegister(Message m, int myPid) {
    Topic t = (Topic) m.body;
    TopicRegistration r = new TopicRegistration(m.src, t);
    Message response;

    if (this.topicTable.register(r)) {
      logger.warning(t.topic + " registered on " + this.node.getId() + " by " + m.src.getId());
      response = new Message(Message.MSG_REGISTER_RESPONSE, r);
      response.ackId = m.id;
      response.operationId = m.operationId;
      response.dest = m.src;
      response.src = this.node;
      assert m.src != null;
      logger.info(" responds with REGISTER_RESPONSE");
      sendMessage(response, m.src.getId(), myPid);
    }

    //		handleFind(m, myPid, Util.logDistance(t.getTopicID(), this.node.getId()));
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

      if (scheduled.get(t.getTopic()) != null) {
        int sch = scheduled.get(t.getTopic()) + 1;
        scheduled.put(t.getTopic(), sch);
      } else {
        scheduled.put(t.getTopic(), 1);
      }
    }
    Timeout timeout = new Timeout(t, m.src.getId());
    EDSimulator.add(
        KademliaCommonConfig.AD_LIFE_TIME, timeout, Util.nodeIdtoNode(this.node.getId()), myPid);
  }

  protected void handleTopicQuery(Message m, int myPid) {

    Topic t = (Topic) m.body;
    TopicRegistration[] registrations = this.topicTable.getRegistration(t, m.src);
    BigInteger[] neighbours =
        this.routingTable.getNeighbours(Util.logDistance(t.getTopicID(), this.node.getId()));

    logger.info(
        "Topic query received at node "
            + this.node.getId()
            + " "
            + registrations.length
            + " "
            + neighbours.length);

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

  private void handleTimeout(Timeout t, int myPid) {
    Operation op = this.operations.get(t.opID);
    if (op != null) {
      if (op.type == Message.MSG_FIND && !Util.nodeIdtoNode(t.node).isUp()) {
        if (!Util.nodeIdtoNode(t.node).isUp()) op.available_requests++;
        logger.info(
            "Timeout "
                + t.getType()
                + " "
                + t.opID
                + " "
                + t.node
                + " "
                + Util.nodeIdtoNode(t.node).isUp()
                + " "
                + op.available_requests);
        if (op.available_requests == KademliaCommonConfig.ALPHA) {
          operations.remove(op.operationId);

          if (registrationMap.get(op.operationId) != null) {
            RegisterOperation rop = (RegisterOperation) operations.get(registrationMap.get(t.opID));
            logger.info(
                "Registration operation id "
                    + registrationMap.get(op.operationId)
                    + " "
                    + op.operationId
                    + " "
                    + rop.getTopic().getTopic());
            rop.elaborateResponse(op.getNeighboursList().toArray(new BigInteger[0]));
            startRegistration(rop, myPid);
            registrationMap.remove(op.operationId);
          }
        }
      }
      // logger.warning("Timeout "+t.getType());
      BigInteger unavailableNode = t.node;
      if (op.type == Message.MSG_TOPIC_QUERY) {
        Message m = new Message();
        m.operationId = op.operationId;
        m.type = Message.MSG_TOPIC_QUERY_REPLY;
        m.src = new KademliaNode(unavailableNode);
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

    if (((SimpleEvent) event).getType() == Timeout.TIMEOUT) {
      handleTimeout((Timeout) event, myPid);
      return;
    }

    SimpleEvent s = (SimpleEvent) event;
    if (s instanceof Message) {
      m = (Message) event;
      m.dest = this.node;

      if (m.src != null) logger.info("<- " + m + " " + m.src.getId());
      else logger.info("<- " + m);
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
        handleInitRegister(m, myPid);
        break;

        /*case Timeout.REG_TIMEOUT:

        String topic = ((Timeout) event).topic.getTopic();
        Message message= generateRegisterMessage(topic);
        logger.warning("Timeout "+topic);
           EDSimulator.add(0, message, Util.nodeIdtoNode(this.node.getId()),myPid);

        break;*/

      case Timeout.REG_TIMEOUT:
        KademliaObserver.reportExpiredRegistration(((Timeout) event).topic, this.node.is_evil);

        if (this.nRefresh == 1) {

          String top = ((Timeout) event).topic.getTopic();
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

  /**
   * set the current NodeId
   *
   * @param tmp BigInteger
   */
  public void setNode(KademliaNode node) {
    this.topicTable.setHostID(node.getId());
    super.setNode(node);
  }
}
