package peersim.kademlia;

public class Ticket implements Comparable<Ticket> {

  // waiting time assigned when ticket was created
  private long wait_time;

  // absolute time of REGTOPIC request (issue time of ticket)
  // FIXME rename this issue time
  private long req_time;

  // cummulative waiting time of this node
  private long cum_wait;

  // registration time (once registration is successful)
  private long reg_time;

  // the topic that ticket is valid for
  private Topic topic;

  // the node that obtained the ticket
  private KademliaNode src;

  // Success or failure
  private boolean isRegistrationComplete;

  // rtt delay from source (advertiser) to destination (advertisement medium)
  private long rtt_delay;

  // indicates whether the local queue was full
  // (it's to distinguish between cases where the queue is full for this
  // particular topic rather than globally
  private int topicOccupancy;

  // Message that registered the ticket
  Message m;

  public Ticket(
      Topic topic,
      long req_time,
      long wait_time,
      KademliaNode src,
      long delay,
      int queueOccupancy) {
    this.topic = topic;
    this.req_time = req_time;
    this.wait_time = wait_time;
    this.cum_wait = wait_time + delay + KademliaCommonConfig.ONE_UNIT_OF_TIME;
    this.src = src;
    this.isRegistrationComplete = false;
    this.rtt_delay = delay;
    this.topicOccupancy = queueOccupancy;
  }

  public void setMsg(Message m) {
    this.m = m;
  }

  public Message getMsg() {
    return this.m;
  }

  public KademliaNode getSrc() {
    return this.src;
  }

  public void updateWaitingTime(long delay) {
    this.wait_time = delay;
    this.cum_wait += delay;
    this.cum_wait += this.rtt_delay + KademliaCommonConfig.ONE_UNIT_OF_TIME;
  }

  public Topic getTopic() {
    return topic;
  }

  public void setTopic() {
    this.topic = topic;
  }

  public void setRTT(long delay) {
    this.rtt_delay = delay;
  }

  public long getRTT() {
    return this.rtt_delay;
  }

  public long getWaitTime() {
    return wait_time;
  }

  public void setWaitTime(long wait_time) {
    this.wait_time = wait_time;
  }

  public long getRegTime() {
    return reg_time;
  }

  public void setRegTime(long reg_time) {
    this.reg_time = reg_time;
  }

  public long getReqTime() {
    return req_time;
  }

  public void setReqTime(long req_time) {
    this.req_time = req_time;
  }

  public long getCumWaitTime() {
    return cum_wait;
  }

  public void setCumWaitTime(long cum_wait) {
    this.cum_wait += cum_wait;
  }

  public void setRegistrationComplete(boolean complete) {
    this.isRegistrationComplete = complete;
  }

  public boolean isRegistered() {
    return this.isRegistrationComplete;
  }

  public boolean isRegistrationComplete() {
    return isRegistrationComplete;
  }

  public void setOccupancy(int occupancy) {
    this.topicOccupancy = occupancy;
  }

  public int getOccupancy() {
    return topicOccupancy;
  }

  @Override
  public int compareTo(Ticket other) {
    /*int result = (int) (this.getCumWaitTime() - other.getCumWaitTime());
    result = -1*result; */

    if (this.getCumWaitTime() > other.getCumWaitTime()) return -1;
    else if (this.getCumWaitTime() == other.getCumWaitTime()) return 0;
    else return 1;
  }

  @Override
  public boolean equals(Object o) {

    // If the object is compared with itself then return true
    if (o == this) {
      return true;
    }

    /* Check if o is an instance of Ticket or not
    "null instanceof [type]" also returns false */
    if (!(o instanceof Ticket)) {
      return false;
    }

    // typecast o to Complex so that we can compare data members
    Ticket r = (Ticket) o;

    if (this.src.equals(r.src)) {
      return true;
    }

    return false;
  }
}
