package peersim.kademlia;

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
public class Discv5TrafficGenerator implements Control {

  // ______________________________________________________________________________________________
  /** MSPastry Protocol to act */
  private static final String PAR_PROT = "protocol";

  private boolean first = true;

  /** MSPastry Protocol ID to act */
  private final int pid;

  /** set to keep track of nodes that already initiated a register */
  // private HashMap<Integer, Boolean>();

  // ______________________________________________________________________________________________
  public Discv5TrafficGenerator(String prefix) {
    pid = Configuration.getPid(prefix + "." + PAR_PROT);
  }

  // ______________________________________________________________________________________________
  /**
   * generates a register message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateTopicLookupMessage(Topic topic) {
    Message m = new Message(Message.MSG_INIT_TOPIC_LOOKUP, topic);
    m.timestamp = CommonState.getTime();

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * generates a register message, by selecting randomly the destination.
   *
   * @return Message
   */
  private Message generateRegisterMessage(Topic topic) {
    Message m = Message.makeRegister(topic);
    m.timestamp = CommonState.getTime();
    // System.out.println("Topic id "+topic.topicID);

    return m;
  }

  // ______________________________________________________________________________________________
  /**
   * every call of this control generates and send a topic register message
   *
   * @return boolean
   */
  public boolean execute() {
    if (!first) {
      return false;
    }
    first = false;
    // first = false;
    System.out.println("Discv5 Traffic generator called");
    String topicString = "t" + Integer.toString(1);
    Topic topic = new Topic(topicString);
    Node registrant;
    do {
      registrant = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((registrant == null) || (!registrant.isUp()));

    Node searcher;
    do {
      searcher = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((searcher == null) || (!searcher.isUp()));

    // send register message

    KademliaProtocol registrantProtocol = (KademliaProtocol) registrant.getKademliaProtocol();
    registrantProtocol.getNode().setTopic(topicString, registrant);
    EDSimulator.add(0, generateRegisterMessage(topic), registrant, pid);

    // send lookup message after some time
    EDSimulator.add(
        KademliaCommonConfig.AD_LIFE_TIME, generateTopicLookupMessage(topic), registrant, pid);

    // send topic lookup message
    /*do {
    	start = Network.get(CommonState.r.nextInt(Network.size()));
    } while ((start == null) || (!start.isUp()));
          EDSimulator.add(1, generateTopicLookupMessage(), start, pid);*/

    return false;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
