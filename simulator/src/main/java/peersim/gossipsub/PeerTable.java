package peersim.gossipsub;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;

public class PeerTable {

  private HashMap<String, HashSet<BigInteger>> peerMap; // , samplesIndexed;

  public PeerTable() {

    peerMap = new HashMap<>();
  }

  public void addPeer(String topic, BigInteger peer) {
    if (peerMap.get(topic) != null) {
      HashSet<BigInteger> nodes = peerMap.get(topic);
      nodes.add(peer);
    } else {
      HashSet<BigInteger> nodes = new HashSet<>();
      nodes.add(peer);
      peerMap.put(topic, nodes);
    }
  }

  public HashSet<BigInteger> getPeers(String topic) {
    return peerMap.get(topic);
  }

  public HashSet<BigInteger> getNPeers(String topic, int n, HashSet<BigInteger> peers) {
    HashSet<BigInteger> nodes = new HashSet<>();
    if (peerMap.get(topic) != null) {
      HashSet<BigInteger> topicPeers = peerMap.get(topic);
      for (BigInteger id : topicPeers) {
        if (!peers.contains(id)) nodes.add(id);
      }
    }

    return nodes;
  }
}
