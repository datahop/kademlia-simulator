package peersim.kademlia.discv5;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
// import java.util.Random;
import java.util.logging.Logger;
import peersim.kademlia.RoutingTable;
import peersim.kademlia.Util;

public class SearchTable extends RoutingTable {

  private Discv5Protocol protocol;

  private int myPid;

  boolean pendingLookup;

  boolean refresh;

  // just for statistics
  HashMap<Integer, Integer> removedPerDist;
  HashSet<BigInteger> added;

  Logger logger;

  public SearchTable(
      int nBuckets,
      int k,
      int maxReplacements,
      Discv5Protocol protocol,
      Topic t,
      int myPid,
      boolean refresh) {

    super(nBuckets, k, maxReplacements);

    this.protocol = protocol;

    this.nodeId = t.getTopicID();

    this.myPid = myPid;

    this.refresh = refresh;

    removedPerDist = new HashMap<Integer, Integer>();
    logger = Logger.getLogger(protocol.getKademliaProtocol().getNode().getId().toString());
    // LogManager.getLogManager().reset();
    added = new HashSet<BigInteger>();
  }

  // add a neighbour to the correct k-bucket
  public boolean addNeighbour(BigInteger node) {
    boolean result = false;
    if (!added.contains(node)) {
      result = super.addNeighbour(node);

      if (result) {
        added.add(node);
      }
    }
    return result;
  }

  // add a neighbour to the correct k-bucket
  public void addNeighbour(BigInteger[] node) {
    for (BigInteger dest : node) {
      addNeighbour(dest);
    }
  }

  // remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {

    getBucket(node).removeNeighbour(node);

    int dist = Util.distance(nodeId, node);
    if (!removedPerDist.containsKey(dist)) {
      removedPerDist.put(dist, 1);
    } else {
      removedPerDist.put(dist, removedPerDist.get(dist) + 1);
    }

    int i = Util.distance(nodeId, node) - bucketMinDistance - 1;
    // BigInteger randomNode = generateRandomNode(i);
    // protocol.refreshBucket(this, randomNode,i);
    protocol.refreshBucket(this, i);
  }

  public int getnBuckets() {
    return nBuckets;
  }
}
