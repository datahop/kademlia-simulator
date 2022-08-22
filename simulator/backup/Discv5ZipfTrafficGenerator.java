package peersim.kademlia;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.math3.distribution.ZipfDistribution;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

/**
 * This control generates random search traffic from nodes to random destination node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class Discv5ZipfTrafficGenerator implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_ZIPF = "zipf";

  private static final String PAR_TOPICNUM = "topicnum";
  private static final String PAR_ATTACKTOPIC = "attackTopic";
  private static final String PAR_LOOKUPS = "randomlookups";

  // private final static String PAR_FREQ = "maxfreq";

  protected boolean first = true;
  protected final int attackTopicIndex; // index of the topic attacked by all the malicious nodes
  protected ZipfDistribution zipf;
  /** MSPastry Protocol ID to act */
  private final int topicNum;

  protected final double exp;

  private int pendingRegistrations, pendingLookups, topicCount;

  // private Map<String,Integer> topicList;

  // private Iterator<Entry<String, Integer>> it;

  private int randomLookups = 0;
  // ______________________________________________________________________________________________
  public Discv5ZipfTrafficGenerator(String prefix) {
    exp = Configuration.getDouble(prefix + "." + PAR_ZIPF);
    topicNum = Configuration.getInt(prefix + "." + PAR_TOPICNUM, 1);
    zipf = new ZipfDistribution(topicNum, exp);
    attackTopicIndex = Configuration.getInt(prefix + "." + PAR_ATTACKTOPIC, -1);
    randomLookups = Configuration.getInt(prefix + "." + PAR_LOOKUPS, 0);

    pendingRegistrations = topicNum;
    pendingLookups = topicNum;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  /*protected Message generateFindNodeMessage() {
  	// existing active destination node
  	Node n = Network.get(CommonState.r.nextInt(Network.size()));

         BigInteger dst = n.getKademliaProtocol().getNode().getId();

  	Message m = Message.makeInitFindNode(dst);
  	m.timestamp = CommonState.getTime();

  	return m;
  }*/

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  protected Message generateFindNodeMessage() {
    // existing active destination node

    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    BigInteger rand = urg.generate();

    Message m = Message.makeInitFindNode(rand);
    m.timestamp = CommonState.getTime();

    return m;
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

  public void emptyBufferCallback(Node n, Topic t) {
    System.out.println("Emptybuffer:" + n.getKademliaProtocol().getNode().getId());
    // EDSimulator.add(0,generateTopicLookupMessage(t.getTopic()),n,
    // n.getKademliaProtocol().getProtocolID());

  }

  // ______________________________________________________________________________________________
  /**
   * generates a topic lookup message, by selecting randomly the destination and one of previousely
   * registered topic.
   *
   * @return Message
   */
  protected Message generateTopicLookupMessage(String topic) {
    // System.out.println("New lookup message "+topic);

    Topic t = new Topic(topic);
    Message m = new Message(Message.MSG_INIT_TOPIC_LOOKUP, t);
    m.timestamp = CommonState.getTime();

    return m;
  }

  public BigInteger getClosestNode(BigInteger id) {
    BigInteger closestId = null;
    for (int i = 0; i < Network.size(); i++) {
      Node node = Network.get(i);
      BigInteger nId = node.getKademliaProtocol().getNode().getId();
      if (closestId == null || (Util.logDistance(id, closestId) > (Util.logDistance(id, nId)))) {
        closestId = nId;
      }
    }
    return closestId;
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {
    System.out.println("Discv5Zipf Traffic generator called");
    first = false;
    HashMap<String, Integer> n = new HashMap<String, Integer>();
    if (first) {
      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        KademliaProtocol prot = (KademliaProtocol) start.getKademliaProtocol();
        Topic t = null;
        String topic = "";

        if (prot.getNode().is_evil) {
          t = prot.getTargetTopic();
        }
        if (t == null) {
          topic = new String("t" + zipf.sample());
          t = new Topic(topic);
        } else {
          topic = t.getTopic();
        }

        Integer value = n.get(topic);
        if (value == null) n.put(topic, 1);
        else {
          int val = value.intValue() + 1;
          n.put(topic, val);
        }
        System.out.println("Topic " + topic + " will be registered ");
        System.out.println("Topic hash: " + t.getTopicID());
        System.out.println("Closest node is " + getClosestNode(t.getTopicID()));
        Message registerMessage = generateRegisterMessage(t.getTopic());

        // kad.setClient(this);
        prot.getNode().setTopic(topic, start);
        // prot.getNode().setCallBack(this,start,t);

        Message lookupMessage = generateTopicLookupMessage(t.getTopic());

        if (registerMessage != null) {
          int time = CommonState.r.nextInt(KademliaCommonConfig.AD_LIFE_TIME);
          // int time = 0;
          System.out.println(
              "Topic "
                  + topic
                  + " will be registered by "
                  + prot.getNode().getId()
                  + " at "
                  + time);
          EDSimulator.add(
              time, registerMessage, start, start.getKademliaProtocol().getProtocolID());
          EDSimulator.add(
              KademliaCommonConfig.AD_LIFE_TIME + time,
              lookupMessage,
              start,
              start.getKademliaProtocol().getProtocolID());
        }
      }

      for (Map.Entry<String, Integer> i : n.entrySet())
        System.out.println("Topic " + i.getKey() + " " + i.getValue() + " times");
      first = false;
    } else if (randomLookups == 1) {

      for (int i = 0; i < Network.size(); i++) {
        for (int j = 0; j < 3; j++) {
          Node start = Network.get(i);
          Message lookup = generateFindNodeMessage();
          EDSimulator.add(0, lookup, start, start.getKademliaProtocol().getProtocolID());
        }
      }
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
