package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import peersim.core.CommonState;

public class Discv5TopicTable implements TopicTable {

  private int capacity = KademliaCommonConfig.TOPIC_TABLE_CAP;
  private int size = 0;

  private SortedMap<Topic, List<TopicRegistration>> table;
  private BigInteger hostID;

  public Discv5TopicTable(BigInteger hostID) {
    table = new TreeMap<Topic, List<TopicRegistration>>();
    this.hostID = hostID;
  }

  public Discv5TopicTable() {
    table = new TreeMap<Topic, List<TopicRegistration>>();
  }

  private void add(TopicRegistration r, Topic t) {

    if (!table.containsKey(t)) {
      List list = new ArrayList<TopicRegistration>();
      list.add(r);
      table.put(t, list);
    } else {
      table.get(t).add(r);
    }

    this.size++;
  }

  public boolean register(TopicRegistration ri) {
    // need to create a copy here. Without it - the topic/registration class would be shared among
    // all the class where it's registered
    Topic t = new Topic(ri.getTopic().getTopic());
    t.setHostID(this.hostID);
    TopicRegistration r = new TopicRegistration(ri);
    r.setTimestamp(CommonState.getTime());

    // check if we already have this registration
    List<TopicRegistration> regList = table.get(t);
    if ((regList != null) && (regList.contains(r))) {
      // System.out.println("We already have topic " + t.getTopic());
      return true;
    }

    // if we have space, always add the registration
    if (size < capacity) {
      // System.out.println(hostID + "Size lower than capacity - adding");
      add(r, t);
      return true;
      // table is full
    } else {
      // new topic is further closer/equal distance from the hostID than the furthest one currently
      // in table
      // if (t.compareTo(table.lastKey()) >= 0) {
      // System.out.println("The topic is closer than another one - replacing");
      table.get(table.lastKey()).remove(0);
      // if a topic has no more registration - remove it
      if (table.get(table.lastKey()).size() == 0) table.remove(table.lastKey());
      this.size--;
      add(r, t);

      return true;
      // }
    }

    // return false;
  }

  public TopicRegistration[] getRegistration(Topic t, KademliaNode src) {
    // TODO check: we might be returning expired registrations, we shoud update the table
    Topic topic = new Topic(t.topic);
    topic.hostID = this.hostID;

    if (table.containsKey(topic)) {

      List<TopicRegistration> topicQ = table.get(topic);

      // Random selection of K results
      TopicRegistration[] results =
          (TopicRegistration[]) topicQ.toArray(new TopicRegistration[topicQ.size()]);
      // List<TopicRegistration> resultsList = Arrays.asList(results);
      List<TopicRegistration> resultsList =
          new ArrayList(
              Arrays.asList(results)); // need to wrap the Arrays.asList in a new List, otherwise
      // iter.remove() below crashes

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

      return final_results;

    } else {
      return new TopicRegistration[0];
    }
  }

  /*public TopicRegistration[] getRegistration(Topic t){
  	Topic t1 = new Topic(t);
  	t1.hostID = this.hostID;
      if(table.containsKey(t1)){
      	List<TopicRegistration> list;
      	if(table.get(t1).size()>=16)
      		list = table.get(t1).subList(0, 16);
      	else
      		list = table.get(t1);
          return (TopicRegistration[]) list.toArray(new TopicRegistration[list.size()]);
      }

      return new TopicRegistration[0];
  }*/

  public HashMap<Topic, Integer> getRegbyTopic() {
    HashMap<Topic, Integer> regByTopic = new HashMap<Topic, Integer>();
    for (List<TopicRegistration> t : table.values()) {
      regByTopic.put(t.get(0).getTopic(), t.size());
    }

    return regByTopic;
  }

  /*public HashMap<BigInteger,Integer> getRegbyRegistrant(){
      HashMap<BigInteger,Integer> regByRegistrant = new HashMap<BigInteger,Integer>();
  	 for(List<TopicRegistration> t: table.values())
       {
  		for(TopicRegistration reg : t)
  		{
  			int count=0;
  			if(regByRegistrant.get(reg.getNode().getId())!=null)count=regByRegistrant.get(reg.getNode().getId());
  			count++;
  			regByRegistrant.put(reg.getNode().getId(),count);
  		}
       }
  	 //System.out.println("Table "+hostID+" "+count);
      return regByRegistrant;

  }*/

  /*public int getRegbyRegistrar(){
  	int count=0;
  	 for(List<TopicRegistration> t: table.values())
       {
       	count+=t.size();
       }
  	 //System.out.println("Table "+hostID+" "+count);
      return count;

  }*/

  public int getCapacity() {
    return this.capacity;
  }

  public void setCapacity(int capacity) {
    this.capacity = capacity;
  }

  public int getSize() {
    return this.size;
  }

  public void setHostID(BigInteger id) {
    this.hostID = id;
  }

  public BigInteger getHostID() {
    return this.hostID;
  }

  public void clear() {
    this.table.clear();
    this.size = 0;
  }

  public String toString() {
    // need a final variable inside lambda expressions below
    final StringBuilder result = new StringBuilder();
    result.append("--------------------------------\n");
    result.append(
        "Proposal1Topic Table size: "
            + this.size
            + "/"
            + this.capacity
            + " hostID: "
            + this.hostID);
    this.table.forEach(
        (k, v) -> {
          result.append("\n" + k.toString() + ":");
          v.forEach(
              (TopicRegistration reg) -> {
                result.append(" " + reg.toString());
              });
        });
    result.append("\n--------------------------------");
    return result.toString();
  }

  public String dumpRegistrations() {
    String result = "";

    for (Topic topic : table.keySet()) {
      List<TopicRegistration> regList = table.get(topic);
      for (TopicRegistration reg : regList) {
        result += this.hostID + ",";
        result += reg.getTopic().getTopic() + ",";
        result += reg.getNode().getId() + ",";
        result += reg.getTimestamp() + "\n";
      }
    }
    return result;
  }

  public HashMap<String, Integer> getRegbyRegistrar() {
    HashMap<String, Integer> topicOccupancy = new HashMap<String, Integer>();
    for (Topic t : table.keySet()) {
      Iterator<TopicRegistration> iterate_value = table.get(t).iterator();
      int count = 0;
      while (iterate_value.hasNext()) {
        if (!iterate_value.next().getNode().is_evil) count++;
      }
      if (table.get(t).size() > 0) topicOccupancy.put(t.getTopic(), count);
    }

    return topicOccupancy;
  }

  public HashMap<String, Integer> getRegEvilbyRegistrar() {
    HashMap<String, Integer> topicOccupancy = new HashMap<String, Integer>();
    for (Topic t : table.keySet()) {
      Iterator<TopicRegistration> iterate_value = table.get(t).iterator();
      int count = 0;
      while (iterate_value.hasNext()) {
        if (iterate_value.next().getNode().is_evil) count++;
      }
      if (table.get(t).size() > 0) topicOccupancy.put(t.getTopic(), count);
    }

    return topicOccupancy;
  }

  public HashMap<String, HashMap<BigInteger, Integer>> getRegbyRegistrant() {
    HashMap<String, HashMap<BigInteger, Integer>> regByRegistrant =
        new HashMap<String, HashMap<BigInteger, Integer>>();

    for (Topic t : table.keySet()) {
      Object[] treg = table.get(t).toArray();
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

    for (Topic t : table.keySet()) {
      Object[] treg = table.get(t).toArray();
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

  public double topicTableUtilisation() {

    return ((double) size) / capacity;
  }
}
