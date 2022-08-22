package peersim.kademlia.operations;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import peersim.kademlia.KademliaCommonConfig;
import peersim.kademlia.KademliaNode;
import peersim.kademlia.Message;
import peersim.kademlia.Topic;

public class LookupOperation extends Operation {
  public final Topic topic;
  // private HashMap<KademliaNode,BigInteger> discovered;
  private ArrayList<KademliaNode> discovered;

  private int malQueried;

  private HashSet<BigInteger> nodesAsked;

  // number of times a lookup results from an honest registrar contains only malicious ads
  private int malRespFromHonest;

  public LookupOperation(BigInteger srcNode, Long timestamp, Topic t) {
    super(srcNode, t.getTopicID(), Message.MSG_TOPIC_QUERY, timestamp);
    this.topic = t;
    discovered = new ArrayList<KademliaNode>();
    malQueried = 0;
    nodesAsked = new HashSet<BigInteger>();
    malRespFromHonest = 0;
  }

  public Topic getTopic() {
    return topic;
  }

  public void addAskedNode(BigInteger id) {
    nodesAsked.add(id);
  }

  public void increaseMalRespFromHonest() {
    malRespFromHonest++;
  }

  public boolean nodeAlreadyAsked(BigInteger id) {
    return nodesAsked.contains(id);
  }

  public int askedNodeCount() {
    return nodesAsked.size();
  }

  public int maliciousResponseByHonest() {
    return malRespFromHonest;
  }

  public void addDiscovered(KademliaNode n) {
    // make sure we don't add more than the limit or the same node twice
    if ((discovered.size() < KademliaCommonConfig.TOPIC_PEER_LIMIT) && !discovered.contains(n)) {
      discovered.add(n);
    }
  }

  public boolean isEclipsed() {
    // we're not eclipsed if we didn't discover anyone yet
    if (discovered.size() == 0) {
      return false;
    }
    for (KademliaNode n : discovered) {
      if (!n.is_evil) {
        return false;
      }
    }
    return true;
  }

  public ArrayList<KademliaNode> getDiscovered() {
    return discovered;
  }

  public String discoveredToString() {
    if (discovered.size() == 0) return "";

    String result = "\"";
    boolean first = true;
    for (KademliaNode n : discovered) {
      if (!n.is_evil) {
        if (first) {
          result += n.getId();
          first = false;
        } else {
          result += " " + n.getId();
        }
      }
    }
    result += "\"";
    return result;
  }

  public String discoveredMaliciousToString() {
    if (discovered.size() == 0) return "";

    String result = "\"";
    boolean first = true;
    for (KademliaNode n : discovered) {
      if (n.is_evil) {
        if (first) {
          result += n.getId();
          first = false;
        } else {
          result += " " + n.getId();
        }
      }
    }
    result += "\"";
    return result;
  }

  public void increaseMaliciousQueries() {
    malQueried++;
  }

  public int discoveredCount() {
    assert (discovered.size() <= KademliaCommonConfig.TOPIC_PEER_LIMIT);
    return discovered.size();
  }

  public int goodDiscoveredCount() {
    int numGood = 0;
    for (KademliaNode n : discovered) {
      if (!n.is_evil) numGood++;
    }
    assert (numGood <= KademliaCommonConfig.TOPIC_PEER_LIMIT);
    return numGood;
  }

  public int maliciousDiscoveredCount() {
    int numMalicious = 0;
    for (KademliaNode n : discovered) {
      if (n.is_evil) numMalicious++;
    }
    assert (numMalicious <= KademliaCommonConfig.TOPIC_PEER_LIMIT);
    return numMalicious;
  }

  public int maliciousNodesQueries() {
    return malQueried;
  }
}
