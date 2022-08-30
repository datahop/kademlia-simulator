package peersim.kademlia.discv5;

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
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.Message;

/**
 * This control generates random search traffic from nodes to random destination node.
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */

// ______________________________________________________________________________________________
public class TrafficGeneratorDiscv5 implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocolkad";

  private static final String PAR_PROT_DISCV5 = "protocoldiscv5";
  private static final String PAR_ZIPF = "zipf";
  private static final String PAR_ATTACKTOPIC = "attackTopic";
  private static final String PAR_TOPICNUM = "topicnum";
  /** MSPastry Protocol ID to act */
  private int protocolID;

  private int protocolDiscv5ID;

  private boolean first = true;

  protected ZipfDistribution zipf;

  private final int topicNum;

  protected final double exp;

  // ______________________________________________________________________________________________
  public TrafficGeneratorDiscv5(String prefix) {
    protocolID = Configuration.getPid(prefix + "." + PAR_PROT);
    protocolDiscv5ID = Configuration.getPid(prefix + "." + PAR_PROT_DISCV5);

    exp = Configuration.getDouble(prefix + "." + PAR_ZIPF);
    topicNum = Configuration.getInt(prefix + "." + PAR_TOPICNUM, 1);

    zipf = new ZipfDistribution(topicNum, exp);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a random find node message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateFindNodeMessage() {
    // existing active destination node
    Node n = Network.get(CommonState.r.nextInt(Network.size()));
    while (!n.isUp()) {
      n = Network.get(CommonState.r.nextInt(Network.size()));
    }

    KademliaProtocol kad = (KademliaProtocol) n.getProtocol(protocolID);

    BigInteger dst = kad.getNode().getId();

    Message m = Message.makeInitFindNode(dst);
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

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a random find node message
   *
   * @return boolean
   */
  public boolean execute() {

    HashMap<String, Integer> n = new HashMap<String, Integer>();

    if (first) {

      for (int i = 0; i < Network.size(); i++) {
        Node start = Network.get(i);
        Discv5Protocol prot = (Discv5Protocol) start.getProtocol(protocolDiscv5ID);
        Topic t = null;
        String topic = "";

        if (prot.getKademliaProtocol().getNode().isEvil()) {
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
        // System.out.println("Closest node is " + getClosestNode(t.getTopicID()));
        Message registerMessage = generateRegisterMessage(t.getTopic());

        // kad.setClient(this);
        // prot.getNode().setTopic(topic, start);
        // prot.getNode().setCallBack(this,start,t);

        Message lookupMessage = generateTopicLookupMessage(t.getTopic());

        if (registerMessage != null) {
          int time = CommonState.r.nextInt(KademliaCommonConfig.AD_LIFE_TIME);
          // int time = 0;
          System.out.println(
              "Topic "
                  + topic
                  + " will be registered by "
                  + prot.getKademliaProtocol().getNode().getId()
                  + " at "
                  + time);
          EDSimulator.add(time, registerMessage, start, protocolDiscv5ID);
          EDSimulator.add(
              KademliaCommonConfig.AD_LIFE_TIME + time, lookupMessage, start, protocolDiscv5ID);
        }
      }

      for (Map.Entry<String, Integer> i : n.entrySet())
        System.out.println("Topic " + i.getKey() + " " + i.getValue() + " times");
      for (int i = 0; i < Network.size(); i++) {
        for (int j = 0; j < 3; j++) {
          int time = CommonState.r.nextInt(300000);
          Node start = Network.get(i);
          Message lookup = generateFindNodeMessage();
          EDSimulator.add(time, lookup, start, protocolID);
        }
      }

      first = false;
    }
    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
