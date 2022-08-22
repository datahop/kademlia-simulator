package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.HashMap;
import peersim.core.CommonState;

// This topic table ensures that subsequent waiting times returned to users
// follow a linear reduction trend by maintaining state of previously computed
// modifiers for users.
public class Discv5StatefulTopicTable extends Discv5GlobalTopicTable {

  // Last computed modifier for each IP, ID, and topic
  private HashMap<String, Double> ip_last_modifier;
  private HashMap<BigInteger, Double> id_last_modifier;
  private HashMap<Topic, Double> topic_last_modifier;
  private double base_last_modifier;
  // The time of last modifier computation for each IP, ID, and topic
  private HashMap<String, Long> ip_timestamp;
  private HashMap<BigInteger, Long> id_timestamp;
  private HashMap<Topic, Long> topic_timestamp;
  private double base_timestamp;

  public Discv5StatefulTopicTable() {
    super();

    // Modifer state initialisation
    // ip_counter = new HashMap<String, Integer>();

    ip_last_modifier = new HashMap<String, Double>();
    id_last_modifier = new HashMap<BigInteger, Double>();
    topic_last_modifier = new HashMap<Topic, Double>();

    ip_timestamp = new HashMap<String, Long>();
    id_timestamp = new HashMap<BigInteger, Long>();
    topic_timestamp = new HashMap<Topic, Long>();

    base_last_modifier = 0;
    base_timestamp = 0;
  }

  protected long getWaitingTime(TopicRegistration reg, long curr_time, Ticket ticket) {
    long waiting_time = 0;
    double baseWaitingTime;
    long cumWaitingTime = 0;

    if (allAds.size() == 0) return 0;
    if (ticket != null) // if this is the first (initial) ticket request, ticket will be null
    cumWaitingTime = ticket.getCumWaitTime() + 2 * ticket.getRTT();

    ArrayDeque<TopicRegistration> topicQ = topicTable.get(reg.getTopic());

    // check if the advertisement already registered before
    if ((topicQ != null) && (topicQ.contains(reg))) {
      // logger.warning("Ad already registered by this node");
      return -1;
    }
    /*else if(allAds.size() == this.tableCapacity) {
        TopicRegistration r = allAds.getFirst();
        long age = curr_time - r.getTimestamp();
        waiting_time = this.adLifeTime - age;
        assert allAds.size() != this.tableCapacity : "Table should not be full";
    }*/
    else {
      // long neededTime  = (long)
      // (Math.max(getTopicModifier(reg)+getIPModifier(reg)+getIdModifier(reg),
      // baseWaitingTime*1/1000000));
      long neededTime =
          (long)
              ((getTopicModifier(reg)
                  + getIPModifier(reg)
                  + getIdModifier(reg)
                  + getBaseModifier()));
      waiting_time = Math.max(0, neededTime - cumWaitingTime);
    }

    waiting_time = Math.min(waiting_time, adLifeTime);
    assert allAds.size() != this.tableCapacity : "Table should not be full";

    return waiting_time;
  }

  protected Ticket[] makeRegisterDecision(long curr_time) {
    Ticket[] tickets = super.makeRegisterDecision(curr_time);

    // Remove expired state (in case of discontinued registrations from Turbulence)
    /* disable removing the state
    Iterator<Entry<Topic, Long>> iter_topic = topic_timestamp.entrySet().iterator();
    while(iter_topic.hasNext()) {
       Entry<Topic, Long> entry = iter_topic.next();
       long timestamp = entry.getValue();
       if (curr_time - timestamp > 2*adLifeTime)
       {
           Topic topic = entry.getKey();
           topic_last_modifier.remove(topic);
           iter_topic.remove();
       }
    }
    Iterator<Entry<String, Long>> iter_ip = ip_timestamp.entrySet().iterator();
    while(iter_ip.hasNext()) {
       Entry<String, Long> entry = iter_ip.next();
       long timestamp = entry.getValue();
       if (curr_time - timestamp > 2*adLifeTime)
       {
           String ip = entry.getKey();
           ip_last_modifier.remove(ip);
           iter_ip.remove();
       }
    }
    Iterator<Entry<BigInteger, Long>> iter_id = id_timestamp.entrySet().iterator();
    while(iter_id.hasNext()) {
       Entry<BigInteger, Long> entry = iter_id.next();
       long timestamp = entry.getValue();
       if (curr_time - timestamp > 2*adLifeTime)
       {
           BigInteger id = entry.getKey();
           id_last_modifier.remove(id);
           iter_id.remove();
       }
    }*/

    return tickets;
  }
  /*
  protected void register(TopicRegistration reg) {
      super.register(reg);
      // update the ip and id counters with the admitted registrations
      String ip = reg.getNode().getAddr();
      Integer ip_count = ip_counter.get(ip);
      if (ip_count == null) {
          ip_counter.put(ip, 1);
      }
      else {
          ip_counter.put(ip, ip_count+1);
      }
      // Add ip address to the Trie
  }*/

  /*
    protected void updateTopicTable(long curr_time) {
  Iterator<TopicRegistration> it = allAds.iterator();
  while (it.hasNext()) {
    		TopicRegistration r = it.next();
        	if (curr_time - r.getTimestamp() >= this.adLifeTime) {
            	ArrayDeque<TopicRegistration> topicQ = topicTable.get(r.getTopic());
             //TopicRegistration r_same = topicQ.pop();
             topicQ.pop();
                //assert r_same.equals(r);
  		it.remove(); //removes from allAds

  	}
  }
    }*/

  /* Get the occupancy score with lower-bound enforced
  protected double getOccupancyScore() {
      double modifier = 0;
      if (allAds.size() > 0) {
          modifier = super.getOccupancyScore();

          // TODO: do we need to do this for the occupancy score
          double delta_time = CommonState.getTime() - base_timestamp;
          double lower_bound = Math.max(0, base_last_modifier - delta_time);
          if (lower_bound < modifier) {
              base_last_modifier = modifier;
              base_timestamp = CommonState.getTime();
          }
          return Math.max(modifier, lower_bound);
      }

      return 0;
  }*/

  protected double getTopicModifier(TopicRegistration reg) {
    double modifier = super.getTopicModifier(reg);
    modifier = modifier * getOccupancyScore() * baseMultiplier;

    // incorporate the lower-bound
    ArrayDeque<TopicRegistration> topicQ = topicTable.get(reg.getTopic());
    Long last_timestamp = topic_timestamp.get(reg.getTopic());
    if (((topicQ != null) && (topicQ.size() > 0)) || last_timestamp != null) {
      double lower_bound = 0;
      if (last_timestamp != null) {
        long delta_time = CommonState.getTime() - last_timestamp;
        lower_bound = Math.max(0, topic_last_modifier.get(reg.getTopic()) - delta_time);
      }
      modifier = Math.max(modifier, lower_bound);
      if (lower_bound < modifier) {
        topic_last_modifier.put(new Topic(reg.getTopic()), modifier);
        topic_timestamp.put(new Topic(reg.getTopic()), CommonState.getTime());
      }
    }

    return modifier;
  }

  protected double getBaseModifier() {
    double modifier = 0;

    if (allAds.size() > 0) {
      modifier = 0.0000001; // XXX old val 0.000001;
      double delta_time = Math.max(0, CommonState.getTime() - base_timestamp);
      double lower_bound = Math.max(0, base_last_modifier - delta_time);
      modifier = modifier * getOccupancyScore() * baseMultiplier;
      if (lower_bound < modifier) {
        base_last_modifier = modifier;
        base_timestamp = CommonState.getTime();
      }
      return Math.max(modifier, lower_bound);
    }

    return 0;
  }

  protected double getIPModifier(TopicRegistration reg) {

    double modifier = super.getIPModifier(reg);
    modifier = modifier * baseMultiplier * getOccupancyScore();

    // Incorporate the lower-bound
    // The lower-bound logic below associates the longest-prefix match
    // with the given registration and stores a lower-bound at that
    // prefix (i.e., the trie node for that prefix).
    // However, this method will fail if a longer ip address is added to the trie (upon a new
    // registration). TODO find a better way to do this.
    String ip = reg.getNode().getAddr();

    TrieNode ancestor = ipMod.getLowerBound(ip);
    long last_timestamp = ancestor.getLastTimestamp();
    double ip_last_modifier = ancestor.getLastModifier();

    long delta_time = CommonState.getTime() - last_timestamp;
    double lower_bound = Math.max(0, ip_last_modifier - delta_time);

    modifier = Math.max(modifier, lower_bound);
    if (lower_bound < modifier) {
      ancestor.setLastTimestamp(CommonState.getTime());
      ancestor.setLastModifier(modifier);
    }

    return modifier;
  }

  protected double getIdModifier(TopicRegistration reg) {
    BigInteger reg_id;

    // Use attackerID for registrant ID if node is evil
    if (reg.getNode().is_evil) reg_id = reg.getNode().getAttackerId();
    else reg_id = reg.getNode().getId();

    Integer id_count = id_counter.get(reg_id);
    if (id_count == null) id_count = 0;

    double modifier = super.getIdModifier(reg);
    modifier = modifier * baseMultiplier * getOccupancyScore();

    // incorporate the lower-bound
    double lower_bound = 0;
    Long last_timestamp = id_timestamp.get(reg_id);
    if (id_count > 0 || last_timestamp != null) {
      if (last_timestamp != null) {
        long delta_time = CommonState.getTime() - last_timestamp;
        lower_bound = Math.max(0, id_last_modifier.get(reg_id) - delta_time);
      }
      modifier = Math.max(modifier, lower_bound);
      if (lower_bound < modifier) {
        id_last_modifier.put(reg_id, modifier);
        id_timestamp.put(reg_id, CommonState.getTime());
      }
    }

    return modifier;
  }
}
