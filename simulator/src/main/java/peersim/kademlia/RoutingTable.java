package peersim.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Gives an implementation for the rounting table component of a kademlia node
 *
 * @author Daniele Furlan, Maurizio Bonani
 * @version 1.0
 */
public class RoutingTable implements Cloneable {

  // node ID of the node
  protected BigInteger nodeId = null;

  // k-buckets
  protected TreeMap<Integer, KBucket> k_buckets = null;

  // number of k-buckets
  protected int nBuckets;

  // bucket size
  protected int k;

  // number of max bucket replacements
  protected int maxReplacements;

  // distance for the lowest bucket
  protected int bucketMinDistance;

  // ______________________________________________________________________________________________
  /** instanciates a new empty routing table with the specified size */
  // public RoutingTable() {
  public RoutingTable(int nBuckets, int k, int maxReplacements) {

    k_buckets = new TreeMap<Integer, KBucket>();
    // initialize k-bukets

    this.nBuckets = nBuckets;

    this.k = k;

    this.maxReplacements = maxReplacements;

    bucketMinDistance = KademliaCommonConfig.BITS - nBuckets;

    for (int i = 0; i <= nBuckets; i++) {
      k_buckets.put(i, new KBucket());
    }
  }

  // add a neighbour to the correct k-bucket
  public boolean addNeighbour(BigInteger node) {
    // add the node to the k-bucket
    return bucketAtDistance(Util.logDistance(nodeId, node)).addNeighbour(node);
  }

  // remove a neighbour from the correct k-bucket
  public void removeNeighbour(BigInteger node) {

    // remove the node from the k-bucket
    bucketAtDistance(Util.logDistance(nodeId, node)).removeNeighbour(node);
  }

  // return the neighbours with a specific common prefix len
  public BigInteger[] getNeighbours(final int dist) {
    BigInteger[] result = new BigInteger[0];
    ArrayList<BigInteger> resultList = new ArrayList<BigInteger>();
    resultList.addAll(bucketAtDistance(dist).neighbours.keySet());

    if (resultList.size() < k && (dist + 1) <= 256) {
      resultList.addAll(bucketAtDistance(dist + 1).neighbours.keySet());
      while (resultList.size() > k) resultList.remove(resultList.size() - 1);
    }
    if (resultList.size() < k & (dist - 1) >= 0) {
      resultList.addAll(bucketAtDistance(dist - 1).neighbours.keySet());
      while (resultList.size() > k) resultList.remove(resultList.size() - 1);
    }
    return resultList.toArray(result);
  }

  // return the closest neighbour to a key from the correct k-bucket
  public BigInteger[] getNeighbours(final BigInteger key, final BigInteger src) {
    // resulting neighbours
    BigInteger[] result = new BigInteger[KademliaCommonConfig.K];

    // neighbour candidates
    ArrayList<BigInteger> neighbour_candidates = new ArrayList<BigInteger>();

    // get the lenght of the longest common prefix
    int prefix_len = Util.logDistance(nodeId, key);

    if (prefix_len < 0) return new BigInteger[] {nodeId};
    // return the k-bucket if is full
    if (bucketAtDistance(prefix_len).neighbours.size() >= KademliaCommonConfig.K) {
      return bucketAtDistance(prefix_len).neighbours.keySet().toArray(result);
    }

    // else get k closest node from all k-buckets
    prefix_len = 0;
    while (prefix_len < KademliaCommonConfig.BITS) {
      neighbour_candidates.addAll(bucketAtDistance(prefix_len).neighbours.keySet());
      // remove source id
      neighbour_candidates.remove(src);
      prefix_len++;
    }

    // create a map (distance, node)
    TreeMap<Integer, List<BigInteger>> distance_map = new TreeMap<Integer, List<BigInteger>>();

    for (BigInteger node : neighbour_candidates) {
      if (distance_map.get(Util.logDistance(node, key)) == null) {
        List<BigInteger> l = new ArrayList<BigInteger>();
        l.add(node);
        distance_map.put(Util.logDistance(node, key), l);

      } else {
        distance_map.get(Util.logDistance(node, key)).add(node);
      }
    }

    List<BigInteger> bestNeighbours = new ArrayList<BigInteger>();
    for (List<BigInteger> list : distance_map.values()) {
      for (BigInteger i : list) {
        if (bestNeighbours.size() < KademliaCommonConfig.K) bestNeighbours.add(i);
        else break;
      }
    }
    if (bestNeighbours.size() < KademliaCommonConfig.K)
      result = new BigInteger[bestNeighbours.size()];

    return bestNeighbours.toArray(result);
  }

  // ______________________________________________________________________________________________
  public Object clone() {
    RoutingTable dolly = new RoutingTable(nBuckets, k, maxReplacements);
    for (int i = 0; i < k_buckets.size(); i++) {
      k_buckets.put(i, new KBucket()); // (KBucket) k_buckets.get(i).clone());
    }
    return dolly;
  }

  // ______________________________________________________________________________________________
  /**
   * print a string representation of the table
   *
   * @return String
   */
  public String toString() {
    return "";
  }

  public KBucket getBucket(BigInteger node) {
    return bucketAtDistance(Util.logDistance(nodeId, node));
  }

  public int getBucketNum(BigInteger node) {
    int dist = Util.logDistance(nodeId, node);
    if (dist <= bucketMinDistance) {
      return 0;
    }
    return dist - bucketMinDistance - 1;
  }

  protected KBucket bucketAtDistance(int distance) {

    if (distance <= bucketMinDistance) {
      return k_buckets.get(0);
    }

    return k_buckets.get(distance - bucketMinDistance - 1);
  }

  public int getbucketMinDistance() {
    return bucketMinDistance;
  }

  public void setNodeId(BigInteger id) {
    this.nodeId = id;
  }

  public BigInteger getNodeId() {
    return this.nodeId;
  }

  // ______________________________________________________________________________________________

} // End of class
// ______________________________________________________________________________________________
