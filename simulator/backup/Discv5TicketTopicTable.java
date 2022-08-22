package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import peersim.core.CommonState;

public class Discv5TicketTopicTable implements TopicTable { // implements TopicTable {

  protected int tableCapacity = KademliaCommonConfig.TOPIC_TABLE_CAP;
  protected int adsPerQueue = KademliaCommonConfig.ADS_PER_QUEUE;
  protected int adLifeTime = KademliaCommonConfig.AD_LIFE_TIME;
  protected BigInteger hostID;

  protected HashMap<String, Integer> ticketCompetingList;

  // Per-topic registration table
  protected HashMap<Topic, ArrayDeque<TopicRegistration>> topicTable;
  // Competing tickets for each topic
  protected HashMap<Topic, ArrayList<Ticket>> competingTickets;
  // All topic advertisements ordered by registration
  protected ArrayDeque<TopicRegistration> allAds;
  protected HashMap<Topic, Long> nextDecisionTime;

  protected Logger logger;
  protected long lastUpdateTime = -1;

  public Discv5TicketTopicTable() {
    topicTable = new HashMap<Topic, ArrayDeque<TopicRegistration>>();
    competingTickets = new HashMap<Topic, ArrayList<Ticket>>();
    allAds = new ArrayDeque<TopicRegistration>();
    nextDecisionTime = new HashMap<Topic, Long>();
    ticketCompetingList = new HashMap<String, Integer>();
    // System.out.println("adLifeTime "+adLifeTime);
    // LogManager.getLogManager().reset();
  }

  public void setHostID(BigInteger id) {
    this.hostID = id;
    logger = Logger.getLogger(id.toString());
  }

  public void setAdLifeTime(int duration) {
    this.adLifeTime = duration;
  }

  public int getAdLifeTime() {
    return this.adLifeTime;
  }

  public int getCapacity() {
    return this.tableCapacity;
  }

  public void setCapacity(int capacity) {
    this.tableCapacity = capacity;
  }

  public void setAdsPerQueue(int qSize) {
    this.adsPerQueue = qSize;
  }

  protected void updateTopicTable(long curr_time) {
    if (this.lastUpdateTime >= curr_time) return;
    this.lastUpdateTime = curr_time;

    Iterator<TopicRegistration> it = allAds.iterator();
    while (it.hasNext()) {
      TopicRegistration r = it.next();
      if (curr_time - r.getTimestamp() >= this.adLifeTime) {
        ArrayDeque<TopicRegistration> topicQ = topicTable.get(r.getTopic());
        // TopicRegistration r_same = topicQ.pop();
        topicQ.pop();
        // assert r_same.equals(r);
        it.remove(); // removes from allAds
      }
    }
  }

  private Ticket getBestTicket(Topic topic) {
    ArrayList<Ticket> ticketList = competingTickets.get(topic);
    if (ticketList == null) return null;
    else if (ticketList.size() == 0) return null;
    else {
      Collections.sort(ticketList);
      // assert ticketList.get(0).getCumWaitTime() >=
      // ticketList.get(ticketList.size()-1).getCumWaitTime();
      return ticketList.get(0);
    }
  }

  private boolean add_to_competingTickets(Topic topic, Ticket ticket) {
    ArrayList<Ticket> ticketList = competingTickets.get(topic);
    if (ticketList == null) {
      ArrayList<Ticket> newTicketList = new ArrayList<Ticket>();
      newTicketList.add(ticket);
      competingTickets.put(topic, newTicketList);
      return true;
    } else {
      if (!ticketList.contains(ticket)) {
        ticketList.add(ticket);
        return true;
      }
    }
    return false;
  }

  protected int topicQueueOccupancy(Topic topic) {
    ArrayDeque<TopicRegistration> topicQ = topicTable.get(topic);
    if (topicQ != null) {
      // System.out.println("topicQ: " + topicQ.size() + "/" + this.adsPerQueue);
      return topicQ.size();
    }
    /*
       else {
    	System.out.println("Topic queue is null. Topics in the table: ");
    	for(Topic t: topicTable.keySet()) {
    		System.out.println(t.getTopic());
    	}

    }*/
    return 0;
  }

  protected long getWaitingTime(TopicRegistration reg, long curr_time) {
    // System.out.println("Get Waiting time "+reg.getTopic().getTopic());

    ArrayDeque<TopicRegistration> topicQ = topicTable.get(reg.getTopic());
    long waiting_time;

    // System.out.println("Topic "+reg.getTopic().topic+" "+topicQ.size());
    /*if(topicQ!=null) {
    for(Iterator<TopicRegistration> itr = topicQ.iterator();itr.hasNext();)  {
    	TopicRegistration t = itr.next();
        System.out.println(t.getTopic().getTopic()+" "+t.getNode().getId());
     }
    }*/
    // check if the advertisement already registered before
    if ((topicQ != null) && (topicQ.contains(reg))) {
      // logger.warning("Ad already registered by this node");
      return -1;
    }
    // if (topicQ != null)
    //    assert topicQ.size() <= this.adsPerQueue;

    // compute the waiting time
    if (topicQ != null && topicQ.size() == this.adsPerQueue) {
      TopicRegistration r = topicQ.getFirst();
      long age = curr_time - r.getTimestamp();
      waiting_time = this.adLifeTime - age;
    } else if (allAds.size() == this.tableCapacity) {
      TopicRegistration r = allAds.getFirst();
      long age = curr_time - r.getTimestamp();
      waiting_time = this.adLifeTime - age;
    } else {
      waiting_time = 0;
    }

    // assert waiting_time <= this.adLifeTime && waiting_time >= 0;

    return waiting_time;
  }

  public double topicTableUtilisation() {

    return ((double) this.allAds.size()) / this.tableCapacity;
  }

  public boolean register_ticket(Ticket ticket, Message m, long curr_time) {
    Topic ti = ticket.getTopic();
    Topic topic = new Topic(ti.topic);
    // topic.setHostID(this.hostID);

    ticket.setMsg(m);
    // if(add_to_competingTickets(topic, ticket))
    //    System.out.println("Ticket for topic: " + ticket.getTopic().getTopic() + " is
    // successful");
    add_to_competingTickets(topic, ticket);

    return setDecisionTime(ticket.getTopic(), curr_time + KademliaCommonConfig.ONE_UNIT_OF_TIME);
  }

  public boolean register(TopicRegistration reg) {
    ArrayDeque<TopicRegistration> topicQ = this.topicTable.get(reg.getTopic());
    // System.out.println("Registering ip: " + reg.getNode().getAddr());
    if (topicQ != null) {
      topicQ.add(reg);
      // System.out.println(this +" Add topictable "+reg.getTopic().getTopic()+" "+topicQ.size());
    } else {
      ArrayDeque<TopicRegistration> q = new ArrayDeque<TopicRegistration>();
      q.add(reg);
      this.topicTable.put(reg.getTopic(), q);
    }

    this.allAds.add(reg);

    return true;
  }

  protected void makeRegisterDecisionForSingleTicket(Ticket ticket, long curr_time) {

    updateTopicTable(curr_time);
    TopicRegistration reg = new TopicRegistration(ticket.getSrc(), ticket.getTopic(), curr_time);
    reg.setTimestamp(curr_time);
    long waiting_time = getWaitingTime(reg, curr_time);

    int topicOccupancy = 0;
    if (this.topicTable.get(reg.getTopic()) != null)
      topicOccupancy = this.topicTable.get(reg.getTopic()).size();

    if (waiting_time == -1) {
      // rejected because a registration from ticket src for topic already exists
      ticket.setRegistrationComplete(false);
      ticket.setWaitTime(waiting_time);
    } else if ((waiting_time == 0)
        && (topicOccupancy < adsPerQueue)
        && (this.allAds.size() < tableCapacity)) { // accepted ticket
      register(reg);
      ticket.setRegistrationComplete(true);
      KademliaObserver.reportCumulativeTime(
          ticket.getTopic(), ticket.getCumWaitTime(), ticket.getSrc().is_evil);
    } else { // waiting_time > 0, reject (for now) due to space
      waiting_time = (waiting_time - ticket.getRTT() > 0) ? waiting_time - ticket.getRTT() : 0;
      ticket.updateWaitingTime(waiting_time);
      ticket.setRegistrationComplete(false);
    }
    KademliaObserver.reportWaitingTime(ticket.getTopic(), waiting_time, ticket.getSrc().is_evil);
  }

  protected Ticket[] makeRegisterDecision(long curr_time) {
    // Determine which topics are up for decision

    ticketCompetingList.clear();
    HashSet<Topic> topicSet = new HashSet<Topic>();
    for (Topic topic : this.competingTickets.keySet()) {
      ArrayList<Ticket> tickets = this.competingTickets.get(topic);
      if (tickets != null && !tickets.isEmpty()) topicSet.add(topic);
    }
    if (topicSet.isEmpty()) {
      return new Ticket[0];
    }

    // list of tickets to respond with MSG_REGISTER_RESPONSE
    // ArrayList<Ticket> responseList = new ArrayList<Ticket>();

    ArrayList<Ticket> ticketList = new ArrayList<Ticket>();
    for (Topic topic : topicSet) {
      ArrayList<Ticket> tickets = this.competingTickets.get(topic);
      // System.out.println("Get Competing by topic "+topic.getTopic()+" "+tickets.size());
      ticketCompetingList.put(topic.getTopic(), tickets.size());
      ticketList.addAll(tickets);
      nextDecisionTime.remove(topic);
      competingTickets.remove(topic);
    }
    Collections.sort(ticketList);
    updateTopicTable(curr_time);

    // Register as many tickets as possible (subject to availability of space in the table)
    for (Ticket ticket : ticketList) {
      makeRegisterDecisionForSingleTicket(ticket, curr_time);
      /*
         TopicRegistration reg = new TopicRegistration(ticket.getSrc(), ticket.getTopic(), curr_time);
         reg.setTimestamp(curr_time);
         long waiting_time = getWaitingTime(reg, curr_time);

      int topicOccupancy = 0;
         if(this.topicTable.get(reg.getTopic())!=null)
             topicOccupancy = this.topicTable.get(reg.getTopic()).size();

         if (waiting_time == -1) {
             // rejected because a registration from ticket src for topic already exists
             ticket.setRegistrationComplete(false);
             ticket.setWaitTime(waiting_time);
         }
         else if ( (waiting_time == 0) && (topicOccupancy < adsPerQueue) && (this.allAds.size() < tableCapacity) ) { //accepted ticket
             register(reg);
             ticket.setRegistrationComplete(true);
             KademliaObserver.reportCumulativeTime(ticket.getTopic(), ticket.getCumWaitTime(), ticket.getSrc().is_evil);
         }
         else { //waiting_time > 0, reject (for now) due to space
             waiting_time = (waiting_time - ticket.getRTT() > 0) ? waiting_time - ticket.getRTT() : 0;
             ticket.updateWaitingTime(waiting_time);
             ticket.setRegistrationComplete(false);


         }
         KademliaObserver.reportWaitingTime(ticket.getTopic(), waiting_time, ticket.getSrc().is_evil);
         */
    }
    Ticket[] tickets = (Ticket[]) ticketList.toArray(new Ticket[ticketList.size()]);
    return tickets;
  }

  protected Ticket getTicket(Topic t, KademliaNode advertiser, long rtt_delay, long curr_time) {
    Topic topic = new Topic(t.topic);
    // topic.setHostID(this.hostID);
    // System.out.println("Get ticket "+topic.getTopic() + " " + this.hostID);
    TopicRegistration reg = new TopicRegistration(advertiser, topic, curr_time);

    // update the topic table (remove expired advertisements)
    updateTopicTable(curr_time);

    // compute ticket waiting time
    long waiting_time = getWaitingTime(reg, curr_time);
    int queueOccupancy = topicQueueOccupancy(t);
    KademliaObserver.reportWaitingTime(topic, waiting_time, advertiser.is_evil);

    if (waiting_time == -1) {
      // already registered
      return new Ticket(topic, curr_time, waiting_time, advertiser, rtt_delay, queueOccupancy);
    }

    waiting_time = (waiting_time - rtt_delay > 0) ? waiting_time - rtt_delay : 0;

    return new Ticket(topic, curr_time, waiting_time, advertiser, rtt_delay, queueOccupancy);
  }

  // Returns true if there is no makeRegisterDecisionForTopic scheduled for the topic at decision
  // time
  public boolean setDecisionTime(Topic topic, long decisionTime) {
    Long time = nextDecisionTime.get(topic);

    if (time == null) {
      nextDecisionTime.put(topic, new Long(decisionTime));
      return true;
    } else if (time > decisionTime) {
      nextDecisionTime.put(topic, new Long(decisionTime));
      return true;
    } else if (time == decisionTime) return false;
    else {
      return true;
    }
  }

  public TopicRegistration[] getRegistration(Topic t, KademliaNode src) {
    // TODO check: we might be returning expired registrations, we shoud update the table
    Topic topic = new Topic(t.topic);
    ArrayDeque<TopicRegistration> topicQ = topicTable.get(topic);

    if (topicQ == null || topicQ.size() == 0) {
      // TODO remove the check below:
      /*
      for(Topic ti: topicTable.keySet()) {
             if (ti.getTopic() == topic.getTopic()) {
                 logger.warning("Error in topic table lookup !");
                 String result = "Unable to find identical topics: ";
                 result += topic.toString();
                 result += "\n";
                 result += ti.toString();
                 result += "\n";
                 //System.out.println(result);
             }
         }*/
      return new TopicRegistration[0];
    }

    // Random selection of K results
    TopicRegistration[] results =
        (TopicRegistration[]) topicQ.toArray(new TopicRegistration[topicQ.size()]);
    List<TopicRegistration> resultsList =
        new ArrayList(
            Arrays.asList(
                results)); // need to wrap the Arrays.asList in a new List, otherwise iter.remove
    // crashes
    // Remove src from the results
    for (Iterator<TopicRegistration> iter = resultsList.listIterator(); iter.hasNext(); ) {
      TopicRegistration reg = iter.next();

      if (reg.getNode().equals(src)) {
        iter.remove();
      }
    }
    if (resultsList.size() == 0) return new TopicRegistration[0];

    int result_len =
        KademliaCommonConfig.MAX_TOPIC_REPLY > resultsList.size()
            ? resultsList.size()
            : KademliaCommonConfig.MAX_TOPIC_REPLY;
    TopicRegistration[] final_results = new TopicRegistration[result_len];

    for (int i = 0; i < result_len; i++) {
      int indexToPick = CommonState.r.nextInt(resultsList.size());
      final_results[i] = resultsList.get(indexToPick);
      resultsList.remove(indexToPick);
    }

    // return (TopicRegistration []) result.toArray(new TopicRegistration[result.size()]);
    return final_results;
  }
  // TODO
  public String dumpRegistrations() {
    String result = "";
    for (Topic topic : topicTable.keySet()) {
      ArrayDeque<TopicRegistration> regQ = topicTable.get(topic);
      for (TopicRegistration reg : regQ) {
        result += this.hostID + ",";
        result += reg.getTopic().getTopic() + ",";
        result += reg.getNode().getId() + ",";
        result += reg.getTimestamp() + "\n";
      }
    }
    return result;
  }

  public float percentMaliciousRegistrations() {
    int num_registrations = 0;
    int num_evil = 0;
    for (Topic topic : topicTable.keySet()) {
      ArrayDeque<TopicRegistration> regQ = topicTable.get(topic);
      for (TopicRegistration reg : regQ) {
        num_registrations += 1;
        KademliaNode n = reg.getNode();
        if (n.is_evil) num_evil += 1;
      }
    }
    return ((float) num_evil) / num_registrations;
  }

  public HashMap<Topic, Integer> getRegbyTopic() {
    // System.out.println("Reg by topic");
    HashMap<Topic, Integer> regByTopic = new HashMap<Topic, Integer>();
    for (Topic t : topicTable.keySet()) {
      ArrayDeque<TopicRegistration> regs = topicTable.get(t);

      regByTopic.put(t, regs.size());
    }

    return regByTopic;
  }

  public HashMap<Topic, Integer> getCompetingTicketsbyTopic() {
    // System.out.println("Reg by topic");
    HashMap<Topic, Integer> numOfCompetingTickets = new HashMap<Topic, Integer>();
    for (Topic t : topicTable.keySet()) {
      ArrayList<Ticket> tickets = competingTickets.get(t);

      int size;
      if (tickets == null) size = 0;
      else if (tickets.size() == 0) size = 0;
      else size = tickets.size();

      numOfCompetingTickets.put(t, size);
    }

    return numOfCompetingTickets;
  }

  /*public HashMap<String, Integer> getRegbyRegistrar(){
  	HashMap<String, Integer> topicOccupancy= new HashMap<String, Integer>();
  	for(Topic t: topicTable.keySet())
  		if(topicTable.get(t).size()>0)topicOccupancy.put(t.getTopic(),topicTable.get(t).size());
      return topicOccupancy;
  }*/

  public HashMap<String, Integer> getRegbyRegistrar() {
    HashMap<String, Integer> topicOccupancy = new HashMap<String, Integer>();
    for (Topic t : topicTable.keySet()) {
      Iterator<TopicRegistration> iterate_value = topicTable.get(t).iterator();
      int count = 0;
      while (iterate_value.hasNext()) {
        if (!iterate_value.next().getNode().is_evil) count++;
      }
      if (topicTable.get(t).size() > 0) topicOccupancy.put(t.getTopic(), count);
    }

    return topicOccupancy;
  }

  public HashMap<String, Integer> getRegEvilbyRegistrar() {
    HashMap<String, Integer> topicOccupancy = new HashMap<String, Integer>();
    for (Topic t : topicTable.keySet()) {
      Iterator<TopicRegistration> iterate_value = topicTable.get(t).iterator();
      int count = 0;
      while (iterate_value.hasNext()) {
        if (iterate_value.next().getNode().is_evil) count++;
      }
      if (topicTable.get(t).size() > 0) topicOccupancy.put(t.getTopic(), count);
    }

    return topicOccupancy;
  }

  public HashMap<String, HashMap<BigInteger, Integer>> getRegbyRegistrant() {
    HashMap<String, HashMap<BigInteger, Integer>> regByRegistrant =
        new HashMap<String, HashMap<BigInteger, Integer>>();

    for (Topic t : topicTable.keySet()) {
      Object[] treg = topicTable.get(t).toArray();
      HashMap<BigInteger, Integer> register = new HashMap<BigInteger, Integer>();
      for (Object reg : treg) {
        int count = 0;
        if (register.get(((TopicRegistration) reg).getNode().getId()) != null)
          count = register.get(((TopicRegistration) reg).getNode().getId());
        if (!((TopicRegistration) reg).getNode().is_evil) count++;
        register.put(((TopicRegistration) reg).getNode().getId(), count);
        // System.out.println("Table "+((TopicRegistration)reg).getNode().getId()+" "+count);
      }
      regByRegistrant.put(t.getTopic(), register);
    }
    return regByRegistrant;
  }

  public HashMap<String, HashMap<BigInteger, Integer>> getRegEvilbyRegistrant() {
    HashMap<String, HashMap<BigInteger, Integer>> regByRegistrant =
        new HashMap<String, HashMap<BigInteger, Integer>>();

    for (Topic t : topicTable.keySet()) {
      Object[] treg = topicTable.get(t).toArray();
      HashMap<BigInteger, Integer> register = new HashMap<BigInteger, Integer>();
      for (Object reg : treg) {
        int count = 0;
        if (register.get(((TopicRegistration) reg).getNode().getId()) != null)
          count = register.get(((TopicRegistration) reg).getNode().getId());
        if (((TopicRegistration) reg).getNode().is_evil) count++;
        register.put(((TopicRegistration) reg).getNode().getId(), count);
        // System.out.println("Table "+((TopicRegistration)reg).getNode().getId()+" "+count);
      }
      regByRegistrant.put(t.getTopic(), register);
    }
    return regByRegistrant;
  }

  public HashMap<String, Integer> getCompetingTickets() {

    return ticketCompetingList;
  }
}
