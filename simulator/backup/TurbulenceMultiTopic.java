package peersim.kademlia;

import com.google.common.net.InetAddresses;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.math3.distribution.ZipfDistribution;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.edsim.EDSimulator;

public class TurbulenceMultiTopic extends Turbulence {

  protected ZipfDistribution zipf;
  /** MSPastry Protocol to act */
  private static final String PAR_MINTOPIC = "mintopicnum";

  private static final String PAR_MAXTOPIC = "maxtopicnum";
  private static final String PAR_ZIPF = "zipf";
  private final int randomLookups;
  private static final String PAR_LOOKUPS = "randomlookups";

  /** MSPastry Protocol ID to act */
  private final int mintopicNum;

  private final int maxtopicNum;

  protected final double exp;

  public TurbulenceMultiTopic(String prefix) {
    super(prefix);
    exp = Configuration.getDouble(prefix + "." + PAR_ZIPF);
    mintopicNum = Configuration.getInt(prefix + "." + PAR_MINTOPIC, 1);
    maxtopicNum = Configuration.getInt(prefix + "." + PAR_MAXTOPIC);
    zipf = new ZipfDistribution(maxtopicNum, exp);
    randomLookups = Configuration.getInt(prefix + "." + PAR_LOOKUPS, 0);

    // TODO Auto-generated constructor stub
  }

  public boolean add() {

    // Add Node
    Node newNode = (Node) Network.prototype.clone();
    for (int j = 0; j < inits.length; ++j) inits[j].initialize(newNode);
    Network.add(newNode);

    int count = 0;
    for (int i = 0; i < Network.size(); ++i) if (Network.get(i).isUp()) count++;

    Random random = new Random();

    String ipString = InetAddresses.fromInteger(random.nextInt()).getHostAddress();

    System.out.println("Adding node " + count + " " + ipString);

    // System.out.println("Adding node " + Network.size());

    // get kademlia protocol of new node
    KademliaProtocol newKad = (KademliaProtocol) (newNode.getProtocol(kademliaid));
    newNode.setKademliaProtocol(newKad);
    // set node Id
    UniformRandomGenerator urg =
        new UniformRandomGenerator(KademliaCommonConfig.BITS, CommonState.r);
    KademliaNode node = new KademliaNode(urg.generate(), ipString, 0);
    ((KademliaProtocol) (newNode.getProtocol(kademliaid))).setNode(node);
    newKad.setProtocolID(kademliaid);
    if (newKad instanceof Discv5EvilTicketProtocol) {
      node.is_evil = true;
    } else if (newKad instanceof Discv5TicketProtocol) {
      node.is_evil = false;
    }

    System.out.println("Turbulence add " + node.getId());

    // sort network
    sortNet();

    addRandomConnections(newNode, 50);
    addNearNodes(newNode, 50);

    int numTopics;

    int num_topics;
    List<String> topicList = new ArrayList<String>(); // references to topics of a node

    numTopics = zipf.sample();
    if (numTopics < mintopicNum) numTopics = mintopicNum;
    for (int i = 0; i < numTopics; i++) {
      String t = "";
      do {
        int num = CommonState.r.nextInt(numTopics) + 1;
        t = new String("t" + num);
      } while (topicList.contains(t));
      topicList.add(t);
    }
    System.out.println(
        "Assigning to node " + newKad.getNode().getId() + " " + topicList.size() + " topics");

    node.setTopicList(topicList, newNode);

    for (int i = 0; i < topicList.size(); i++) {
      // System.out.print(topicList.get(i)+" ");
      Message registerMessage = generateRegisterMessage(topicList.get(i));
      Message lookupMessage = generateTopicLookupMessage(topicList.get(i));
      // Message initLookupMessage = generateFindNodeMessage(new Topic(topicList.get(i)));

      if (registerMessage != null) {
        System.out.println(
            "Topic " + topicList.get(i) + " will be registered by " + newKad.getNode().getId());
        // EDSimulator.add(0, initLookupMessage, newNode, kademliaid);
        EDSimulator.add(10000, registerMessage, newNode, kademliaid);
        EDSimulator.add(10000, lookupMessage, newNode, kademliaid);
      }
    }

    if (randomLookups == 1) {

      for (int j = 0; j < 3; j++) {
        Message lookup = generateFindNodeMessage();
        EDSimulator.add(0, lookup, newNode, newNode.getKademliaProtocol().getProtocolID());
      }
    }
    // System.out.println();

    return false;
  }
}
