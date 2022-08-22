package peersim.kademlia;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

// Round-robin Discv5 Topic table
public class Discv5RRTopicTable extends Discv5TicketTopicTable { // implements TopicTable

  private SortedMap<Topic, Long> registrationTimes;
  // index to the current topic in the round-robin
  boolean roundRobin;
  Long endOfRoundTime; // time of next round start
  Long startOfRoundTime; // time of current round start
  int nextTopicIndex; // current topic index in the round-robin order

  public Discv5RRTopicTable() {
    super();
    this.registrationTimes = new TreeMap<Topic, Long>();
    this.roundRobin = false;
    this.endOfRoundTime = Long.valueOf(-1);
    this.startOfRoundTime = Long.valueOf(-1);
    this.nextTopicIndex = -1;
    this.adsPerQueue =
        this.tableCapacity; // this is needed to avoid tickets waiting for full topic queue (not
    // full table) and attempting to register in the middle of round robin
  }

  // Returns the current time when xth space is available in the topic table.
  // If slot_number is one, then this returns the time that first space opens up in topic table
  private long getTimeOfNextKAvailableSlot(int slot_number, long curr_time) {
    int remaining_capacity = this.tableCapacity - this.allAds.size();

    if (remaining_capacity >= slot_number) return curr_time;

    if (slot_number > this.tableCapacity) {
      // This can happen if numberOfTopics > tableCapacity, but otherwise shouldn't
      logger.severe("Requested slot is greater than topic table capacity");
      slot_number = this.tableCapacity;
    }

    Iterator<TopicRegistration> it = allAds.iterator();
    int count = remaining_capacity;
    long waiting_time = 0;
    for (TopicRegistration reg : allAds) {
      waiting_time = this.adLifeTime - (curr_time - reg.getTimestamp());
      count += 1;
      if (count == slot_number) break;
    }

    return curr_time + waiting_time;
  }

  protected long getRRWaitingTime(Topic topicToRegister, long curr_time) {
    assert this.roundRobin : "get method is only to be called in round-robin mode";

    Long reg_time = this.registrationTimes.get(topicToRegister);
    if (reg_time != null && reg_time > curr_time) {
      return reg_time - curr_time;
    }

    assert this.endOfRoundTime >= curr_time : "end of round time must be later than now";
    return this.endOfRoundTime - curr_time;
  }

  protected long getWaitingTime(TopicRegistration reg, long curr_time) {
    Topic topicToRegister = reg.getTopic();
    if (this.roundRobin) {
      // in round robin mode
      // System.out.println("Entered round-robin!");
      Long reg_time = this.registrationTimes.get(topicToRegister);
      if (reg_time != null && reg_time > curr_time) {
        return reg_time - curr_time; // - KademliaCommonConfig.ONE_UNIT_OF_TIME;
      } else {
        return this.endOfRoundTime - curr_time; // - KademliaCommonConfig.ONE_UNIT_OF_TIME;
      }
    } else { // not in round-robin mode (table is not full)
      return super.getWaitingTime(reg, curr_time);
    }
  }

  private int getNumberOfCompetingTickets() {
    int num_tickets = 0;
    for (Map.Entry<Topic, ArrayList<Ticket>> entry : this.competingTickets.entrySet()) {
      ArrayList<Ticket> ticket_list = entry.getValue();
      num_tickets += ticket_list.size();
    }
    return num_tickets;
  }

  private int getNumberOfCompetingTopics() {

    int num_topics = 0;
    for (Map.Entry<Topic, ArrayList<Ticket>> entry : this.competingTickets.entrySet()) {
      ArrayList<Ticket> ticket_list = entry.getValue();
      if (ticket_list.size() > 0) num_topics += 1;
    }

    return num_topics;
  }

  protected Ticket[] makeRegisterDecision(long curr_time) {
    // update topic table (remove expired ads)
    updateTopicTable(curr_time);
    int remaining_capacity = this.tableCapacity - this.allAds.size();
    int num_competing_topics = getNumberOfCompetingTopics();
    // System.out.println("Number of competing topics: " + num_competing_topics);
    // System.out.println("Remaining capacity: " + remaining_capacity);
    boolean begin_of_round = false;
    if (this.roundRobin
        && (num_competing_topics > remaining_capacity)
        && (this.nextTopicIndex >= this.registrationTimes.keySet().size())) {
      begin_of_round = true;
      scheduleRoundRobin(curr_time);
    } else if (!this.roundRobin && (num_competing_topics > remaining_capacity)) {
      this.roundRobin = true;
      begin_of_round = true;
      scheduleRoundRobin(curr_time);
    }
    if (this.roundRobin) return makeRegisterDecisionRoundRobin(curr_time, begin_of_round);
    else return super.makeRegisterDecision(curr_time);
  }

  // Make registration decision in round-robin order
  // NOTE: adsPerQueue limit is not used when determining registration
  private Ticket[] makeRegisterDecisionRoundRobin(long curr_time, boolean begin_of_round) {
    Topic current_topic = null;
    int remaining_capacity = this.tableCapacity - this.allAds.size();
    ArrayList<Ticket> all_tickets = new ArrayList<Ticket>();
    int topic_index = this.nextTopicIndex;
    int num_competing_tickets = getNumberOfCompetingTickets();
    boolean end_of_round = false;

    // If beginning of round, go through all the competing tickets for all topics;
    // otherwise, loop as many as the remaining capacity
    while (remaining_capacity > 0 || begin_of_round) {

      if (remaining_capacity > 0 && end_of_round) {
        // End of round reached but there is still space in the table,
        // so start a new round.
        scheduleRoundRobin(curr_time);
        this.nextTopicIndex = 0;
        topic_index = 0;
        int num_topics = getNumberOfCompetingTopics();
        if (num_topics <= remaining_capacity) this.roundRobin = false;
        else begin_of_round = true;
        // Fix the waiting time of tickets that are rejected in the
        // previous round but waiting time is set to 0 (because
        // endOdRoundTime coincided with last topic of round)
        for (Ticket t : all_tickets) {
          if (!t.isRegistered() && (t.getWaitTime() == 0)) {
            long waiting_time = getRRWaitingTime(t.getTopic(), curr_time);
            t.setWaitTime(waiting_time);
            KademliaObserver.reportWaitingTime(t.getTopic(), waiting_time, t.getSrc().is_evil);
          }
        }

        end_of_round = false;
      }

      if (num_competing_tickets == 0) break;

      // Determine which topic's turn in the round-robin order
      // The ordering of the keys in the TreeMap determines the order of topics.
      int indx = 0;
      for (Topic topic : this.registrationTimes.keySet()) {
        if (indx == topic_index) {
          current_topic = topic;
          break;
        }
        indx++;
      }
      assert current_topic != null : "current topic should not be null";

      ArrayList<Ticket> ticket_list = new ArrayList<Ticket>();
      ticket_list.addAll(this.competingTickets.get(current_topic));
      all_tickets.addAll(ticket_list);
      ArrayList<Ticket> competing_tickets_for_topic = this.competingTickets.get(current_topic);
      // this.competingTickets.remove(current_topic);

      // Sort tickets by cumulative waiting time (oldest ticket first)
      Collections.sort(ticket_list);

      // admit first (oldest) ticket, only if there is available capacity
      boolean first = false;
      if (remaining_capacity > 0) first = true;

      for (Ticket ticket : ticket_list) {
        // Admit the first (oldest) ticket in the queue, the rest is assigned a non-zero waiting
        // time.
        if (first && (this.allAds.size() < tableCapacity)) { // accepted ticket
          TopicRegistration reg =
              new TopicRegistration(ticket.getSrc(), ticket.getTopic(), curr_time);
          reg.setTimestamp(curr_time);
          first = false;
          ticket.setRegistrationComplete(true);
          ticket.setWaitTime(0);
          KademliaObserver.reportCumulativeTime(
              ticket.getTopic(), ticket.getCumWaitTime(), ticket.getSrc().is_evil);
          register(reg);
          this.nextTopicIndex += 1;
          competing_tickets_for_topic.remove(ticket);
          num_competing_tickets--;
          remaining_capacity--;
        } else {
          ticket.setRegistrationComplete(false);
          long waiting_time = getRRWaitingTime(ticket.getTopic(), curr_time);
          if (waiting_time != 0) competing_tickets_for_topic.remove(ticket);
          // this.endOfRoundTime - curr_time;
          assert waiting_time >= 0 : "waiting time must be positive or zero";
          // waiting_time -= KademliaCommonConfig.ONE_UNIT_OF_TIME;
          waiting_time = (waiting_time - ticket.getRTT() > 0) ? waiting_time - ticket.getRTT() : 0;
          ticket.setWaitTime(waiting_time);
          KademliaObserver.reportWaitingTime(
              ticket.getTopic(), waiting_time, ticket.getSrc().is_evil);
          num_competing_tickets--;
        }
      }
      topic_index += 1;
      if (topic_index == this.registrationTimes.keySet().size() && begin_of_round)
        begin_of_round = false;
      if (this.nextTopicIndex == this.registrationTimes.keySet().size()) end_of_round = true;
    } // end of while

    for (Ticket t : all_tickets) {
      Topic topic = t.getTopic();
      this.competingTickets.remove(topic);
    }

    Ticket[] tickets = (Ticket[]) all_tickets.toArray(new Ticket[all_tickets.size()]);
    return tickets;
  }

  // Computes a registration time for each topic with non-empty competing tickets.
  // The round-robin registration for each topic is done according to the
  // scheduled registration time.
  private void scheduleRoundRobin(long curr_time) {
    // NOTE: the schedule does not consider adsPerQueue limit

    this.registrationTimes.clear();

    // Identify the topics with competing tickets and add them to Sorted Map
    for (Map.Entry<Topic, ArrayList<Ticket>> entry : this.competingTickets.entrySet()) {
      Topic topic = entry.getKey();
      ArrayList<Ticket> ticket_list = entry.getValue();
      if ((ticket_list != null) && (ticket_list.size() > 0)) {
        this.registrationTimes.put(topic, null);
      }
    }

    // Identify the waiting times for each topic in round-robin order
    int slot = 1;
    this.startOfRoundTime = getTimeOfNextKAvailableSlot(slot, curr_time);
    for (Map.Entry<Topic, Long> entry : this.registrationTimes.entrySet()) {
      long slot_time = getTimeOfNextKAvailableSlot(slot, curr_time);
      entry.setValue(slot_time);
      slot += 1;
      // System.out.println("For topic: " + entry.getKey().getTopic() + " registration to take place
      // at: " + entry.getValue());
    }

    this.endOfRoundTime = getTimeOfNextKAvailableSlot(slot, curr_time);
    // System.out.println("End of round time: " + this.endOfRoundTime);
    this.nextTopicIndex = 0;
  }
}
